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
import random


firefox_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
    "Gecko/20100101 Firefox/124.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.bing.com/",
    "DNT": "1",
}

chrome_win_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
}


def col2int(s):
    return ascii_uppercase.find(s) + 1


def get_symbols(sheet):
    # read symbols from Q2 onwards
    symbols = []
    for i, sym in enumerate(sheet.col_values(17)[1:], start=2):
        symbols.append(f"NSE:{sym}".strip())


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
    # set the received auth code
    fyer_init_session.set_token(auth_code)

    # stage 3
    access_token_json = fyer_init_session.generate_token()
    if "access_token" not in access_token_json:
        print("Error: Failed to get the access token")
        pprint(access_token_json)
        # exit(1)

    # access_token = access_token_json["access_token"]
    # print(access_token[:20])

    # Websocket setup

    # app id
    app_id = "I2UO8QM1WX-100"

    ##### Get symbols from col Q
    symbols = first_sheet.col_values(col2int("Q"))[1:]
    symbols = list(filter(lambda sym: sym, symbols))
    symbols = [f"NSE:{sym}-EQ" for sym in symbols]
    print(symbols)

    class Events:
        @staticmethod
        def on_message(res):
            # print("Response:", res)

            if "symbol" not in res:
                return

            symbol: str = res["symbol"]
            suffix = symbol.removeprefix("NSE:").removesuffix("-EQ")
            cells = first_sheet.findall(suffix)

            vol_traded_today = res["vol_traded_today"]
            ltp = res["ltp"]
            chp = res["chp"]

            update_cells = []

            # Price
            update_cells += [
                gspread.Cell(cell.row, col2int("R"), value=ltp) for cell in cells
            ]

            # Current volume
            update_cells += [
                gspread.Cell(cell.row, col2int("M"), value=vol_traded_today)
                for cell in cells
            ]

            # Stock %
            update_cells += [
                gspread.Cell(cell.row, col2int("C"), value=chp) for cell in cells
            ]
            first_sheet.update_cells(update_cells)
            print(
                f"Fyers: Updated Price = {ltp};  Current Volume = {vol_traded_today}; Stock % = {chp}; for {symbol}"
            )
            sleep(0.5)

        @staticmethod
        def on_error(message):
            print("Error: (can be ignored)", message)

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

    # fyers = fyers_data_ws.FyersDataSocket(
    #     access_token=f"{app_id}:{access_token}",
    #     log_path="",
    #     litemode=False,
    #     write_to_file=False,  # Save response in a log file instead of printing it.
    #     reconnect=True,
    #     on_connect=Events.on_connect,
    #     on_close=Events.on_close,
    #     on_error=Events.on_error,
    #     on_message=Events.on_message,
    # )

    def fyers_process():
        fyers.connect()

    def moneycontrol_process():
        def is_valid_float(x):
            try:
                float(x)
                return True
            except ValueError:
                return False

        def process_indexes():
            indexes = []
            for n, url in enumerate(first_sheet.col_values(col2int("T")), start=1):
                url = str(url).strip()

                if url.startswith(
                    "https://www.moneycontrol.com/markets/index-contribution-"
                ):
                    indexes.append([n, url])

            mc_urls = enumerate(first_sheet.col_values(col2int("J")), start=1)
            mc_urls = list(
                filter(
                    lambda cell: str(cell[1])
                    .strip()
                    .startswith("https://www.moneycontrol.com/india/stockpricequote/"),
                    mc_urls,
                )
            )

            for idx1, idx2 in zip(indexes, indexes[1:]):
                mi, mx = idx1[0], idx2[0]
                idx1.append([cell for cell in mc_urls if cell[0] in range(mi, mx)])

            m = indexes[-1][0]
            indexes[-1].append([cell for cell in mc_urls if cell[0] >= m])

            for idx in indexes:
                n, url, lst = idx

                print(f"Fetching Moneycontrol index: {url}")
                req = requests.get(url)

                bs = BeautifulSoup(req.text, "html.parser")
                tag = bs.find(
                    "script", {"id": "__NEXT_DATA__", "type": "application/json"}
                )

                if tag is None:
                    raise Exception(f"No json found. {url}")

                data = json.loads(tag.text)["props"]["pageProps"]["data"][
                    "contributersDataItem"
                ]
                pos_data = data["positiveContributersArr"]
                neg_data = data["negativeContributersArr"]

                final_data = pos_data + neg_data

                update_cells = []
                for unit in final_data:
                    for row, mc_url in lst:
                        slug = unit["slug"]

                        if slug in mc_url:
                            print(unit["slug"], unit["stock_weight"], f"at row {row}")
                            update_cells.append(
                                gspread.Cell(
                                    row, col2int("B"), value=unit["stock_weight"]
                                )
                            )
                first_sheet.update_cells(update_cells)

        process_indexes()

        def fetch_scids(urls: list[str]) -> dict[str, str]:
            cache_file = Path("moneycontrol-scid.json")
            if cache_file.exists():
                cache = json.load(open(cache_file))
            else:
                cache = {}

            try:
                for url in urls:
                    if url in cache:
                        continue

                    r = requests.get(
                        url,
                        timeout=5,
                        headers=random.choice([firefox_headers, chrome_win_headers]),
                    )
                    if r.status_code != 200:
                        raise Exception()
                    bs = BeautifulSoup(r.text, "html.parser")
                    tag = bs.find(id="sc_id")
                    if tag is None:
                        raise Exception("Cannot find required tag")
                    scid = tag["value"]
                    cache[url] = scid
                    print(f"Fetched SCID for {url}: {scid}")
                    sleep(10)
            except Exception as e:
                print(f"Failed to get SCID for url {e}: {url}")
                exit(1)
            finally:
                json.dump(cache, open(cache_file, "w"), indent=4)

            return cache

        urls = [str(url).strip() for url in first_sheet.col_values(col2int("J"))]

        url_scids = fetch_scids(
            list(
                filter(
                    lambda url: url.startswith(
                        "https://www.moneycontrol.com/india/stockpricequote/"
                    ),
                    urls,
                )
            )
        )

        for url, scid in url_scids.items():
            if url.split("/")[-1] != scid:
                print(
                    f"Soft Warning: Moneycontrol url {url} has a different scid {scid}"
                )

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

                # #### price
                # tag = bs.find(id="nsecp")
                # if tag:
                #     p = str(tag.string).strip().replace(",", "")

                #     if is_valid_float(p):
                #         data["price"] = p
                #     else:
                #         print(f"Warning: price not updated for {url} {p}")
                # else:
                #     print(f"Warning: price not updated for {url}")

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

                print(
                    f"Moneycontrol: Updating {'; '.join({f'{k} = {v}' for k, v in data.items()})} for url {url}"
                )
                return (row, data)
            except Exception:
                return None

        def fetch_data_v2(row, url) -> tuple[int, dict] | None:
            term = url_scids[url]
            url = f"https://priceapi.moneycontrol.com/pricefeed/nse/equitycash/{term}"
            try:
                r = requests.get(url, timeout=1, headers=firefox_headers)
                if r.status_code != 200:
                    return None
                r_json = r.json()
                data = {}

                ###! currently no way to get beta

                #### 20d avg
                data["20d_avg"] = r_json["data"]["DVolAvg20"]

                # Stock %
                data["price_change_percent"] = r_json["data"]["pricepercentchange"]

                # Current Price
                data["current_price"] = r_json["data"]["pricecurrent"]

                # Current Volume
                data["current_volume"] = r_json["data"]["VOL"]

                print(
                    f"Moneycontrol: Updating {'; '.join({f'{k} = {v}' for k, v in data.items()})} for url {url}"
                )
                return (row, data)
            except Exception:
                return None

        while True:
            try:
                batch_size = 15
                for i in range(0, len(row_url), batch_size):
                    subset = row_url[i : i + batch_size]
                    update_cells = []
                    with ThreadPoolExecutor(max_workers=batch_size) as ex:
                        futures = [ex.submit(fetch_data_v2, r, u) for r, u in subset]
                        for f in as_completed(futures):
                            result = f.result()
                            if result and result[1]:
                                row, data = result
                                if "beta" in data:
                                    update_cells.append(
                                        gspread.Cell(
                                            row, col2int("O"), value=data["beta"]
                                        )
                                    )

                                if "20d_avg" in data:
                                    update_cells.append(
                                        gspread.Cell(
                                            row,
                                            col2int("N"),
                                            value=f"{int(float(data['20d_avg'])):,}",
                                        )
                                    )

                                if "price_change_percent" in data:
                                    update_cells.append(
                                        gspread.Cell(
                                            row,
                                            col2int("C"),
                                            value=str(
                                                round(
                                                    float(data["price_change_percent"]),
                                                    2,
                                                )
                                            ),
                                        )
                                    )

                                if "current_price" in data:
                                    update_cells.append(
                                        gspread.Cell(
                                            row,
                                            col2int("R"),
                                            value=f"{float(data['current_price']):,}",
                                        )
                                    )

                                if "current_volume" in data:
                                    update_cells.append(
                                        gspread.Cell(
                                            row,
                                            col2int("M"),
                                            value=f"{int(float(data['current_volume'])):,}",
                                        )
                                    )

                                # print(f"{row}: {data}")

                    if update_cells:
                        first_sheet.update_cells(update_cells)

                sleep(1)
            except Exception as e:
                print(f"Moneycontrol process error: {e}")
                pass

    threads = []
    for process in [
        # fyers_process,
        moneycontrol_process
    ]:
        t = threading.Thread(target=process)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()
