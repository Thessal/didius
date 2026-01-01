from datetime import datetime, timedelta
from trade.webdav import upload
from configs import get_token, telegram
from trade.api import post, get_key, get
from time import sleep
from configs.trading_calendar import TradingDay
import json

info_domain = get_key()["domain"]

query_balance = {
    "url": info_domain + "/uapi/domestic-futureoption/v1/trading/inquire-balance",
    "tr_id": "CTFO6118R",
    "headers": {
        "Content-Type": "application/json; charset=utf-8",
        "custtype": "P",  # 개인고객
        **get_key(),
        "tr_cont": "",  # N : 다음 데이터 조회 (output header의 tr_cont가 M일 경우)
    },
    "params": lambda kwargs: {
        "CANO": "63400867",  # 계좌번호 앞 8자리
        "ACNT_PRDT_CD": "03",  # 계좌번호 뒤 2자리
        "MGNA_DVSN": "02",
        "EXCC_STAT_CD": "1",
        "CTX_AREA_FK200": "",  # 연속조회 관련
        "CTX_AREA_NK200": "",
    },
    "return_value": lambda resp: (
        resp.json(),
        None
    )
}


query_balance_night = {k: v for k, v in query_balance.items()}
query_balance_night.update({
    "url": info_domain + "/uapi/domestic-futureoption/v1/trading/inquire-ngt-balance",
    "tr_id": "CTFN6118R",
})


query_transaction = {
    "url": info_domain + "/uapi/domestic-futureoption/v1/trading/inquire-ccnl",
    "tr_id": "TTTO5201R",
    "headers": {
        "Content-Type": "application/json; charset=utf-8",
        "custtype": "P",  # 개인고객
        **get_key(),
        "tr_cont": "",  # N : 다음 데이터 조회 (output header의 tr_cont가 M일 경우)
    },
    "params": lambda kwargs: {
        "CANO": "63400867",
        "ACNT_PRDT_CD": "03",
        "STRT_ORD_DT": kwargs["yyyymmdd"],
        "END_ORD_DT": kwargs["yyyymmdd"],
        "SLL_BUY_DVSN_CD": "00",  # 00: All, 01: Sell, 02: Buy
        "CCLD_NCCS_DVSN": "00",  # 00: All, 01: Settled, 02: Unsettled
        "SORT_SQN": "DS",  # AS: Ascending, DS: Descending
        "STRT_ODNO": kwargs["odno"],  # Start Order Number (for pagination)
        "PDNO": kwargs["pdno"],  # Product Number (blank for all)
        "MKET_ID_CD": "",
        "CTX_AREA_FK200": "",  # Continuous query key
        "CTX_AREA_NK200": ""  # Continuous query key
    },
    "return_value": lambda resp: (
        resp.json(),
        None
    )
}

query_transaction_night = {k: v for k, v in query_transaction.items()}
query_transaction_night.update({
    "url": info_domain + "/uapi/domestic-futureoption/v1/trading/inquire-ngt-ccnl",
    "tr_id": "STTN5201R",
})


query_price = {
    "url": info_domain + "/uapi/domestic-futureoption/v1/quotations/inquire-asking-price",
    "tr_id": "FHMIF10010000",
    "headers": {
        "Content-Type": "application/json; charset=utf-8",
        "custtype": "P",  # 개인고객
        **get_key(),
        "tr_cont": "",  # N : 다음 데이터 조회 (output header의 tr_cont가 M일 경우)
    },
    "params": lambda kwargs: {
        "FID_COND_MRKT_DIV_CODE": "CM" if kwargs["night"] else "F",
        # F: 지수선물, O:지수옵션
        # JF: 주식선물, JO:주식옵션
        # CF: 상품선물(금), 금리선물(국채), 통화선물(달러)
        # CM: 야간선물, EU: 야간옵션
        "FID_INPUT_ISCD": kwargs["pdno"]
    },
    "return_value": lambda resp: (
        resp.json(),
        None
    )
}
query_trade = {
    "url": info_domain + "/uapi/domestic-futureoption/v1/trading/order",
    "tr_id": "TTTO1101U",
    "headers": {
        "Content-Type": "application/json; charset=utf-8",
        "custtype": "P",  # 개인고객
        **get_key(),
        "tr_cont": "",  # N : 다음 데이터 조회 (output header의 tr_cont가 M일 경우)
    },
    "json": lambda kwargs: {
        "ORD_PRCS_DVSN_CD": "02",
        "CANO": "63400867",  # 계좌번호 앞 8자리
        "ACNT_PRDT_CD": "03",  # 계좌번호 뒤 2자리
        "SLL_BUY_DVSN_CD": kwargs["sell_buy_code"],  # 01 : 매도, 02 : 매수
        "SHTN_PDNO": kwargs["instr_id"],  # "201S03370",
        "ORD_QTY": kwargs["qty"],  # "1"
        "UNIT_PRICE": kwargs["prc"],  # 0 if marker order.
        "NMPR_TYPE_CD": "",  # kwargs["order_type_code"], # 호가유형코드 생략가능
        # "NMPR_TYPE_CD":"", # 호가유형코드 생략가능
        "KRX_NMPR_CNDT_CD": "",  # 한국거래소호가조건코드
        "CTAC_TLNO": "",  # 전화번호
        "FUOP_ITEM_DVSN_CD": "",  # 공란 : 종목구분코드
        "ORD_DVSN_CD": kwargs["order_type_code"]  # "02"
        # 01 : 지정가
        # 02 : 시장가
        # 03 : 조건부
        # 04 : 최유리,
        # 10 : 지정가(IOC)
        # 11 : 지정가(FOK)
        # 12 : 시장가(IOC)
        # 13 : 시장가(FOK)
        # 14 : 최유리(IOC)
        # 15 : 최유리(FOK)
    },
    "return_value": lambda resp: (
        resp.json(),
        None
    )
}
query_trade_night = {k: v for k, v in query_trade.items()}
query_trade_night.update({"tr_id": "STTN1101U", })


query_cancel = {
    "url": info_domain + "/uapi/domestic-futureoption/v1/trading/order-rvsecncl",
    "tr_id": "TTTO1103U",
    "headers": {
        "Content-Type": "application/json; charset=utf-8",
        "custtype": "P",  # 개인고객
        **get_key(),
        "tr_cont": "",  # N : 다음 데이터 조회 (output header의 tr_cont가 M일 경우)
    },
    "json": lambda kwargs: {
        "ORD_PRCS_DVSN_CD": "02",
        "CANO": "63400867",  # 계좌번호 앞 8자리
        "ACNT_PRDT_CD": "03",  # 계좌번호 뒤 2자리
        "RVSE_CNCL_DVSN_CD": kwargs["modify_cancel_code"],  # 01 : 정정, 02 : 취소
        # "SHTN_PDNO": kwargs["instr_id"],# "201S03370",
        "ORGN_ODNO": kwargs["orig_id"],  # 원주문번호
        "ORD_QTY": kwargs["qty"],  # "1"
        "UNIT_PRICE": kwargs["prc"],  # 0 if marker order.
        "NMPR_TYPE_CD": "",  # kwargs["order_type_code"], # 호가유형코드 생략가능
        # "NMPR_TYPE_CD":"", # 호가유형코드 생략가능
        "KRX_NMPR_CNDT_CD": "",  # 한국거래소호가조건코드
        "RMN_QTY_YN": "Y",  # Y:전량 N:일부
        # "CTAC_TLNO":"", # 전화번호
        "FUOP_ITEM_DVSN_CD": "",  # 공란 : 종목구분코드
        # [Header tr_id JTCE1002U(선물옵션 정정취소 야간)]
        # 01 : 선물
        # 02 : 콜옵션
        # 03 : 풋옵션
        # 04 : 스프레드
        "ORD_DVSN_CD": kwargs["order_type_code"]  # "02"
        # 01 : 지정가
        # 02 : 시장가
        # 03 : 조건부
        # 04 : 최유리,
        # 10 : 지정가(IOC)
        # 11 : 지정가(FOK)
        # 12 : 시장가(IOC)
        # 13 : 시장가(FOK)
        # 14 : 최유리(IOC)
        # 15 : 최유리(FOK)
    },
    "return_value": lambda resp: (
        resp.json(),
        None,
    )
}

query_cancel_night = {k: v for k, v in query_cancel.items()}
query_cancel_night.update({"tr_id": "STTN1103U", })


# post
# def get_historical_price(lookup_days = 1, instr_id="201V09365"):
#     dfs = []
#     tz = zoneinfo.ZoneInfo("Asia/Seoul")
#     cur = datetime.now(tz=tz)
#     until = (cur - timedelta(days=lookup_days)).replace(hour=8, minute=45, second=0, microsecond=0)
#     while until<cur:
#         df = get(**apis_get["price"], token=token, instr_id=instr_id, until=cur)
#         dfs.append(df)
#         cur = df.index[-1].to_pydatetime().replace(tzinfo=tz)
#     return pd.concat(dfs)


token = get_token()


instr_buy_id = "_"
instr_buy_exp = "0000-00"
instr_sell_id = "_"
instr_sell_exp = "0000-00"


def sell():
    # Sell
    result = post(**query_trade, token=token, sell_buy_code="01",
                  instr_id=instr_sell_id, qty=str(1), prc="0", order_type_code="02")
    datestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    upload([{"datetime": datestr, "instrument": {"code": instr_sell_id[:3],
           "exp_month": instr_sell_exp}, "amount": -1, "price": 0}], "TRADELIVE")
    telegram("sell " + instr_sell_id)
    print(result)
    print(datetime.now())


def buy():
    # Buy
    result = post(**query_trade, token=token, sell_buy_code="02",
                  instr_id=instr_buy_id, qty=str(1), prc="0", order_type_code="02")
    datestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    upload([{"datetime": datestr, "instrument": {"code": instr_buy_id[:3],
           "exp_month": instr_buy_exp}, "amount": 1, "price": 0}], "TRADELIVE")
    telegram("buy " + instr_buy_id)
    print(result)
    print(datetime.now())


def cancel_limit(orig_id, price, amount, night=False, verbose=True):
    if night:
        query = query_cancel_night
    else:
        query = query_cancel
    result = post(**query, token=token, modify_cancel_code="02",
                  orig_id=orig_id,
                  prc=f"{price:.2f}", qty=str(amount), order_type_code="01", verbose=verbose)

    print(result)

    # Check validity of cancellation
    assert (
        (str(result[0]["msg_cd"]) == 'KIER2320')  # already filled
        or
        (str(result[0]["rt_cd"]) == '0')  # success
    )
    return result


"""
cancel 결과 예시
({'rt_cd': '0', 'msg_cd': 'APBK0029', 'msg1': '주문전송이 정상적으로 처리되었습니다.', 'output': {'ACNT_NAME': '최종국', 'TRAD_DVSN_NAME': '매수', 'ITEM_NAME': '미니 F 202508', 'ORD_TMD': '101229', 'ORD_GNO_BRNO': '91257', 'ORGN_ODNO': '0000001827', 'ODNO': '0000001829'}}, None)
({'rt_cd': '7', 'msg_cd': 'KIER2320', 'msg1': '정정／취소 가능수량이 부족합니다'}, None)
"""


def check_balance(pdno, night=False):
    if night:
        query = query_balance_night
    else:
        query = query_balance
    msg, ret_val = get(**query, token=token, page_limit=0)
    # assert msg["tr_cont"] in ["D", "E"]
    print("new_balance")
    print(msg)
    for record in msg["output1"]:
        if record["shtn_pdno"] == pdno:
            amount = int(record["cblc_qty"])
            if record["sll_buy_dvsn_name"] in ["매수", "BUY"]:
                direction = 1
            elif record["sll_buy_dvsn_name"] in ["매도", "SLL"]:
                direction = -1
            else:
                assert amount == 0
                direction = 0
            return amount * direction
    else:
        return 0


def check_transaction(pdno, odno, night=False, verbose=True):
    if night:
        query = query_transaction_night
        now = datetime.today()
        if now.hour > 17:
            yyyymmdd = TradingDay(now).next(copy=False).strftime()
        else:
            yyyymmdd = TradingDay(now).strftime()
    else:
        query = query_transaction
        yyyymmdd = datetime.today().strftime("%Y%m%d")
    msg, ret_val = get(
        **query, token=token, verbose=verbose,
        pdno=pdno, odno=odno, yyyymmdd=yyyymmdd, page_limit=0
    )
    # assert msg["tr_cont"] in ["D", "E"]
    for record in msg["output1"]:
        if record["odno"] == odno:
            trade_direction = {"01": -1, "02": 1}[record["sll_buy_dvsn_cd"]]
            trade_amount = int(record["tot_ccld_qty"])
            # record["qty"] #잔여수량
            return trade_direction * trade_amount
    else:
        return 0


def parse_msg(raw_result):
    print(raw_result)
    msg, ret_val = raw_result
    return {"order_id": msg["output"]["ODNO"]}


def buy_limit(price, amount, current_balance, wait_time, night=False):
    # Buy
    pdno = instr_buy_id
    instr_exp = instr_buy_exp
    sell_buy_code = "02"
    info_str = "buy"
    new_balance = buysell_limit(
        pdno=pdno, instr_exp=instr_exp, info_str=info_str, sell_buy_code=sell_buy_code,
        price=price, amount=amount, current_balance=current_balance, wait_time=wait_time,
        night=night)
    return new_balance


def sell_limit(price, amount, current_balance, wait_time, night=False):
    # Sell
    pdno = instr_sell_id
    instr_exp = instr_sell_exp
    sell_buy_code = "01"
    info_str = "sell"
    new_balance = buysell_limit(
        pdno=pdno, instr_exp=instr_exp, info_str=info_str, sell_buy_code=sell_buy_code,
        price=price, amount=amount, current_balance=current_balance, wait_time=wait_time,
        night=night)
    return new_balance


def buysell_limit(pdno, instr_exp, info_str, sell_buy_code, price, amount, current_balance, wait_time, night=False):
    if night:
        query = query_trade_night
    else:
        query = query_trade
    result = post(**query, token=token, sell_buy_code=sell_buy_code,
                  instr_id=pdno, prc=f"{price:.2f}", qty=str(amount), order_type_code="01")
    telegram(info_str + " " + pdno)
    odno = parse_msg(result)["order_id"]
    print(odno, type(odno))
    sleep(wait_time)
    try:
        # 이것이 실패할 경우 오더가 계속 쌓이게 되므로 주의
        cancel_result = cancel_limit(odno, price, amount, night=night)
        print(cancel_result)
        print(datetime.now())
        old_balance = current_balance
        trade_amt = check_transaction(pdno, odno, night=night)
        new_balance = check_balance(pdno, night=night)
        print({"old_balance": old_balance,
              "trade_amt": trade_amt, "new_balance": new_balance})
        assert (old_balance + trade_amt == new_balance)
        try:
            datestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if trade_amt != 0:
                upload([{"datetime": datestr, "instrument": {
                       "code": pdno[:3], "exp_month": instr_exp}, "amount": trade_amt, "price": price}], "TRADELIVE")
        except:
            pass
    except Exception as e:
        print(e)
        raise RuntimeError("Limit order logic error")
    return new_balance


def limit_raw(action, price, amount, night=False, verbose=True):
    # 자동취소 기능 없는 limit order 발생 함수 (사용시 주의 바람)
    sell_buy_code = {"sell": "01", "buy": "02"}[action]
    pdno = {"sell": instr_sell_id, "buy": instr_buy_id}[action]
    if night:
        query = query_trade_night
    else:
        query = query_trade
    result = post(**query, token=token, verbose=verbose, sell_buy_code=sell_buy_code,
                  instr_id=pdno, prc=f"{price:.2f}", qty=str(amount), order_type_code="01")
    odno = parse_msg(result)["order_id"]
    return odno


def get_bbo(instr_trade, night=False):
    msg, ret_val = get(**query_price, pdno=instr_trade,
                       token=token, night=night, page_limit=0)
    print(msg["output2"]['aspr_acpt_hour'])
    assert (int(msg["output2"]['aspr_acpt_hour']) >
            int(datetime.now().strftime("%H%M%S")) - 100)
    # FIXME : 야간선물의 경우 날짜가 넘어가서 이걸로 안됨. 날짜 파싱 필요
    return (float(msg["output2"]["futs_bidp1"]), float(msg["output2"]["futs_askp1"]))


def adjust_position(balance, target_balance, wait_time=30, night=False):
    inventory_risk = 0
    balance = int(round(balance))
    target_balance = int(round(target_balance))
    for i in range(100):
        amt = target_balance - balance  # amount to trade
        instr_trade = instr_sell_id if amt < 0 else instr_buy_id
        bb, bo = get_bbo(instr_trade)
        print("[adjust_position] BBO", bb, bo)

        if amt > 0:
            print("[adjust_position] buy_limit")
            balance = buy_limit(bb + 0.02 * (inventory_risk - 1),
                                int(round(abs(amt))), balance, wait_time=wait_time, night=night)
        elif amt < 0:
            print("[adjust_position] sell_limit")
            balance = sell_limit(bo - 0.02 * (inventory_risk - 1),
                                 int(round(abs(amt))), balance, wait_time=wait_time, night=night)
        else:
            break

        if target_balance != balance:
            inventory_risk += 1
        else:
            inventory_risk = 0

        print("Current balance:", balance)
        print("Target balance:", target_balance)
    return balance
