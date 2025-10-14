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


SYMBOLS = ["NSE:SBIN-EQ", "NSE:ADANIENT-EQ"]

TABLE = ["symbol", "vol_traded_todaylast_traded_time", "exch_feed_time"]

if __name__ == "__main__":
    #### Google spreadsheet setup
    gsc_json = json.load(open("secrets/google-service-account.json"))
    gc = gspread.service_account(filename="secrets/google-service-account.json")

    print(
        f"Make sure to share the spreadsheet with the user {gsc_json['client_email']} otherwise it will NOT work"
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

    ####
    print("Updating sheet headers")
    sheet = sh.sheet1

    cells = sheet.range(f"A1:{ascii_uppercase[len(TABLE)]}1")

    for name, cell in zip(TABLE, cells):
        name = name.replace("_", " ").title()
        cell.value = name
    sheet.update_cells(cells)
    ####

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

    class Events:
        @staticmethod
        def on_message(res):
            print("Response:", res)
            if "symbol" not in res:
                return
            symbol = res["symbol"]
            index = SYMBOLS.index(symbol) + 2

            sheet = sh.sheet1

            cells = sheet.range(f"A{index}:{ascii_uppercase[len(TABLE)]}{index}")

            for name, cell in zip(TABLE, cells):
                cell.value = res[name]

            sheet.update_cells(cells)

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
            fyers.subscribe(symbols=SYMBOLS, data_type=data_type)

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

    fyers.connect()

    # print(sh.sheet1.update_acell("A1", "Hello"))
