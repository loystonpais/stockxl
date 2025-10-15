from fyers_apiv3.FyersWebsocket import data_ws as fyers_data_ws
from fyers_apiv3 import fyersModel
from pathlib import Path
import gspread
import json
import tomllib
import webbrowser
from time import sleep
from pprint import pprint
from string import ascii_uppercase
import re
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup


def col2int(s):
    return ascii_uppercase.find(s) + 1


def get_symbols(sheet):
    # read symbols from Q2 onwards
    symbols = []
    for i, sym in enumerate(sheet.col_values(17)[1:], start=2):
        symbols.append(f"NSE:{sym}".strip())


TABLE = ["symbol", "vol_traded_today", "exch_feed_time"]

if __name__ == "__main__":
    #### Google spreadsheet setup
    gsc_json = json.load(open("secrets/google-service-account.json"))
    gc = gspread.service_account(filename="secrets/google-service-account.json")

    print(
        f"Make sure to share the spreadsheet with user {gsc_json['client_email']} otherwise it will NOT work"
    )

    with open("sheet.toml") as f:
        sheet_json = tomllib.loads(f.read())

    if "key" in sheet_json:
        sh = gc.open_by_key(sheet_json["key"])
    elif "url" in sheet_json:
        sh = gc.open_by_url(sheet_json["url"])
    elif "name" in sheet_json:
        sh = gc.open(sheet_json["name"])
    else:
        raise ValueError("sheet.toml has no reqired properties")

    first_sheet = sh.get_worksheet(0)

    #### Fyer setup

    with open("secrets/fyers-cred.toml") as f:
        fyer_cred_json = tomllib.loads(f.read())

    fyer_init_session = fyersModel.SessionModel(
        redirect_uri="https://trade.fyers.in/api-login/redirect-uri/index.html",
        response_type="code",
        grant_type="authorization_code",
        state="xl_dump",
        **fyer_cred_json,
    )

    # If auth token is aleady cached then use it
    cached_auth_code_path = Path("secrets/fyers-auth-code-cache.txt")
    if cached_auth_code_path.exists():
        auth_code = open(cached_auth_code_path).read().strip()
        if not auth_code:
            auth_code = None
    else:
        auth_code = None

    if auth_code is not None:
        print(
            f"Using cached access token. If you want to generate a new one, delete {cached_auth_code_path}"
            " and the run the program again"
        )
    else:
        auth_code_get_url = fyer_init_session.generate_authcode()
        print(f"Redirect URL: {auth_code_get_url}")
        if input("Open in browser? "):
            webbrowser.open(auth_code_get_url, new=1)
        auth_code = input("Please enter the access token: ").strip().replace("\n", "")

        with open(cached_auth_code_path, "w") as f:
            f.write(auth_code)

    ## stage 2
    # set the received access token
    fyer_init_session.set_token(auth_code)

    # stage 3
    access_token_json = fyer_init_session.generate_token()
    if "access_token" not in access_token_json:
        print("Error: Failed to get the access token")
        pprint(access_token_json)
        exit(1)

    access_token = access_token_json["access_token"]
    print(access_token[:20])

    # Websocket setup

    # app id
    app_id = "I2UO8QM1WX-100"

    ##### Get symbols
    symbol = first_sheet.find(re.compile(".*Symbol.*", re.IGNORECASE))
    if symbol is None:
        print("Symbols cell not found")
        exit(1)
    symbols = first_sheet.col_values(symbol.col)
    symbols = list(filter(lambda sym: sym, symbols))
    symbols = [f"NSE:{sym}-EQ" for sym in symbols]
    print(symbols)

    class Events:
        @staticmethod
        def on_message(res):
            print("Response:", res)

            if "symbol" not in res:
                return

            symbol: str = res["symbol"]
            suffix = symbol.removeprefix("NSE:").removesuffix("-EQ")
            cells = first_sheet.findall(suffix)
            current_volume_cell = first_sheet.find(
                re.compile(".*Current volume.*", re.IGNORECASE)
            )

            if current_volume_cell is None:
                print("No cell called Current volume found")
                return

            volume_cell_col = current_volume_cell.col

            update_cells = [
                gspread.Cell(cell.row, volume_cell_col, value=res["vol_traded_today"])
                for cell in cells
            ]
            print("Symbol found! ", update_cells)
            # return
            first_sheet.update_cells(update_cells)
            sleep(1)

        @staticmethod
        def on_error(message):
            print("Error:", message)

        @staticmethod
        def on_close(message):
            print("Connection closed:", message)

        @staticmethod
        def on_connect():
            # Specify the data type and symbols you want to subscribe to
            data_type = "SymbolUpdate"

            # Subscribe to the specified symbols and data type
            fyers.subscribe(symbols=symbols, data_type=data_type)

            # Keep the socket running to receive real-time data
            fyers.keep_running()

    fyers = fyers_data_ws.FyersDataSocket(
        access_token=f"{app_id}:{access_token}",
        log_path="",
        litemode=False,
        write_to_file=False,  # Save response in a log file instead of printing it.
        reconnect=True,
        on_connect=Events.on_connect,
        on_close=Events.on_close,
        on_error=Events.on_error,
        on_message=Events.on_message,
    )

    def fyers_process():
        fyers.connect()
        # while True:
        #    ...

    def moneycontrol_process():
        def is_valid_float(x):
            try:
                float(x)
                return True
            except ValueError:
                return False

        urls = [str(url).strip() for url in first_sheet.col_values(col2int("J"))]
        cells = enumerate(urls, start=1)
        row_url = list(
            filter(
                lambda cell: cell[1].startswith(
                    "https://www.moneycontrol.com/india/stockpricequote/"
                ),
                cells,
            )
        )

        def fetch_data(row, url) -> tuple[int, dict] | None:
            try:
                r = requests.get(url, timeout=5)
                if r.status_code != 200:
                    return None
                bs = BeautifulSoup(r.text, "html.parser")
                data = {}

                #### price
                tag = bs.find(id="nsecp")
                if tag:
                    p = str(tag.string).strip().replace(",", "")

                    if is_valid_float(p):
                        data["price"] = p
                    else:
                        print(f"Warning: price not updated for {url} {p}")
                else:
                    print(f"Warning: price not updated for {url}")

                #### beta
                beta_tag = bs.find(class_="nsebeta")
                if beta_tag:
                    beta = str(beta_tag.string).strip().replace(",", "")
                    if is_valid_float(beta):
                        data["beta"] = beta
                    else:
                        print(f"Warning: beta not updated for {url} {beta}")
                else:
                    print(f"Warning: beta not updated for {url}")

                #### 20d avg
                td_avg_tag = bs.find(class_="nsev20a")
                if td_avg_tag:
                    td_avg = str(td_avg_tag.string).strip().replace(",", "")
                    if td_avg.isnumeric():
                        data["20d_avg"] = td_avg
                    else:
                        print(f"Warning: 20d average not updated for {url} {td_avg}")
                else:
                    print(f"Warning: 20d average not updated for url {url}")

                return (row, data)
            except Exception:
                return None

        while True:
            batch_size = 8
            for i in range(0, len(row_url), batch_size):
                subset = row_url[i : i + batch_size]
                update_cells = []
                with ThreadPoolExecutor(max_workers=batch_size) as ex:
                    futures = [ex.submit(fetch_data, r, u) for r, u in subset]
                    for f in as_completed(futures):
                        result = f.result()
                        if result and result[1]:
                            row, data = result
                            if "price" in data:
                                update_cells.append(
                                    gspread.Cell(row, col2int("R"), value=data["price"])
                                )
                            if "beta" in data:
                                update_cells.append(
                                    gspread.Cell(row, col2int("O"), value=data["beta"])
                                )

                            if "20d_avg" in data:
                                update_cells.append(
                                    gspread.Cell(
                                        row, col2int("N"), value=data["20d_avg"]
                                    )
                                )
                            print(f"{row}: {data}")

                if update_cells:
                    first_sheet.update_cells(update_cells)
                    print(f"Updated {len(update_cells)} rows.")
            sleep(1)

    threads = []
    for process in [fyers_process, moneycontrol_process]:
        t = threading.Thread(target=process)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()
