import pandas as pd
import numpy as np
import time
import warnings

MONTH_CODES = "FGHJKMNQUVXZ"

MONTH_NAMES = [
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
]

MONTH_NUMS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

MONTH_NAME_TO_CODE = {k: v for k, v in zip(MONTH_NAMES, MONTH_CODES)}

FIELDS_MAP = {
    "Trade Date": "date",
    "Risk Free Interest Rate": "RATE",
    "Open Implied Volatility": "PRICE_OPEN",
    "Last Implied Volatility": "PRICE_LAST",
    "High Implied Volatility": "PRICE_HIGH",
    "Previous Close Price": "PRICE_CLOSE_PREV",
    "Close Implied Volatility": "IMPLIEDVOL_BLACK",
    "Strike Price": "STRIKE",
    "Option Premium": "PREMIUM",
    "General Value6": "UNDL_PRICE_SETTLE",
    "General Value7": "UNDL_PRICE_LAST",
}

FLOAT_FIELDS = [
    "PRICE_OPEN",
    "PRICE_LAST",
    "PRICE_HIGH",
    "PRICE_CLOSE_PREV",
    "IMPLIEDVOL_BLACK",
    "PREMIUM",
    "RATE",
    "STRIKE",
    "UNDL_PRICE_SETTLE",
    "UNDL_PRICE_LAST",
]


def transform(raw_data_: pd.DataFrame, instruments_: pd.DataFrame) -> pd.DataFrame:
    """
    Create a function called transform that returns a normalized table.
    Do not mutate the input.
    The runtime of the transform function should be below 1 second.

    :param raw_data_: dataframe of all features associated with instruments, with associated timestamps
    :param instruments_: dataframe of all traded instruments
    """
    #initialize variables
    raw_data, instruments = raw_data_, instruments_
    drop_index, moneyness_list= [], []
    contract_year, contract_month, month_code =[], [], []
    symbol = []
    instruments_dict = {}
    field, value = [], []
    trade_date_warn, expired_instruments_warn, contract_nulls_warn = False, False, False
    #Check if error column in the raw data
    if 'Error' in raw_data.columns:
        error_column = True
    else:
        error_column = False

    #get base list and other info from instruments for later use
    base_list = instruments['Base'].values
    for index, row in instruments.iterrows():
        conc = row['Exchange']+'_'+row['Bloomberg Ticker']
        if len(row['Bloomberg Ticker']) == 1:
            conc += '_'
        instruments_dict[row['Base']] = conc
    #instruction 2
    raw_data['Contract'] = raw_data['Term']

    for index, row in raw_data.iterrows():
        drop = False #trigger for drop or not
        if error_column and pd.isna(row['Error']): #detect error column
            drop = True
        if pd.isna(row['Contract']): #instruction 2 and 5
            if pd.isna(row['Term']):
                drop = True
                contract_nulls_warn = True
            else:
                row['Contract'] = row['Term']
        if pd.isna(row['Trade Date']): #instruction 3
            drop = True
            trade_date_warn = True
        if time.strptime(row['Trade Date'], "%m/%d/%Y") > time.strptime(row['Expiration Date'], "%m/%d/%Y"): #instruction 4
            drop = True
            expired_instruments_warn = True
        if not drop:
            RIC = row['RIC'] #instruction 6 and 7 and 8
            get_base = True
            for i in range(len(RIC)):
                if get_base:
                    RIC_header = RIC[:i]
                    if RIC_header in base_list and RIC[i].isnumeric():
                        base = RIC_header
                        get_base = False
                    j = i
                if not get_base:
                    if RIC[j:i+1].isnumeric():
                        moneyness = RIC[j:i+1]
                    else:
                        break
            moneyness_list.append(moneyness)
            c_year = row['Expiration Date'].split('/')[-1]
            c_month = row['Period'][:-1].upper()
            last_digit = row['Period'][-1]
            if c_year[-1] != last_digit:
                c_year = str(int(c_year)+1)
            contract_month.append(c_month)
            contract_year.append(c_year)
            m_code = MONTH_NAME_TO_CODE[c_month]
            month_code.append(m_code)
        else:
            drop_index.append(index)
        s = 'FUTURE_VOL_'+instruments_dict[base]+m_code+c_year+'_'+moneyness #instruction 10
        symbol.append(s)
    if drop_index:
        data = raw_data.drop(drop_index) #drop rows
        if trade_date_warn:# raise warning
            warnings.warn('Nulls exist in Trade Date')
        if expired_instruments_warn:
            warnings.warn('Expired Instruments exist')
        if contract_nulls_warn:
            warnings.warn('Nulls in Contract Columns')
    else:
        data = raw_data
    data['contract_year'] = contract_year
    data['contract_month'] = contract_month
    data['month_code'] = month_code
    data['symbol']=symbol
    data=data.rename(columns=FIELDS_MAP) #instruction 9
    target_index = data.index.values.tolist()
    for i in range(len(FLOAT_FIELDS)-1):
        target_index+=data.index.values.tolist()
    result = data[['date','symbol']].loc[target_index].reset_index(drop=True) #instruction 11
    result['source'] = ['refinitiv']*len(result)
    target_data = data[FLOAT_FIELDS]
    for i in FLOAT_FIELDS:
        field+=[i]*len(data)
        value += data[i].values.tolist()
    result['field'] = field
    for i in range(len(value)):
        if ',' in str(value[i]):
            value[i] = str(value[i]).replace(',','')
    result['value'] = value
    result['value'] = result['value'].astype(float)# align column type
    result['date']=pd.to_datetime(result['date'])
    return result


if __name__ == '__main__':
    raw_data = pd.read_csv("raw_data.csv")
    instruments = pd.read_csv("instruments.csv")
    st = time.process_time()
    output = transform(raw_data, instruments)
    et = time.process_time()
    print(f"Wall time: {100 * (et-st)} ms")
    expected_output = pd.read_csv(
        "expected_output.csv",
        index_col=0,
        parse_dates=['date']
    )
    pd.testing.assert_frame_equal(output, expected_output)