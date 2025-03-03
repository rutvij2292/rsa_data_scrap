import streamlit as st;
import requests
from bs4 import BeautifulSoup
import pandas as pd

import math
import asyncio
import aiohttp
from aiohttp import ClientSession

import nest_asyncio
nest_asyncio.apply()

main_url = "https://rsa.ed.gov/data/view-submission-rsa-17"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}

column_header = [
    'URL',
    'Financial Management Specialist Contact',
    'Financial Management Specialist Phone',
    'Financial Management Specialist Email',
    '1. Federal Grant Award Number',
    '2. Federal Fiscal Year',
    '3-a. Grant Period From',
    '3-b. Grant Period To',
    '4. Recipient Organization',
    '5. Basis of Accounting',
    '6. Reporting Period End Date',
    '7. Final Report',
    'Due Date',
    '8. UEI/DUNS Number',
    '9. Recipient Account Number or Identifying Number (maximum 30 characters)',
    '10. Total Federal Funds Awarded',
    '11. Federal Cash Receipts',
    '12. Federal Cash Disbursements',
    '13. Federal Cash on Hand',
    '14. Federal Share of Allowable Expenditures',
    '15. Federal Funds Expended for the Provision of Pre-Employment Transition Services (not including expenditures for these services incurred with program income) ',
    # '15-a. Required and Coordination Pre-employment Transition Service Activities and Other VR Services that Support Access to and Participation in Pre-Employment Transition Services',
    # '15-b. Authorized Pre-employment Transition Service Activities',
    '16. Federal Share for Establishment of Facilities for CRP Purposes',
    '17. Federal Share for Construction of Facilities for CRP Purposes',
    '18. Federal Share of Allowable Unliquidated Obligations',
    '19. Total Federal Share',
    '20. Unobligated Balance of Federal Funds',
    '21. Total Federal Program Income Received',
    '22. Program Income Expended Under VR Program in Accordance with the Additional Alternative',
    '23. VR SSA Payments Transferred to the State Independent Living Services Program' ,
    '24. VR SSA Payments Transferred to the Independent Living Services for Older Individuals who are Blind Program' ,
    '25. VR SSA Payments Transferred to the Client Assistance Program' ,
    '26. VR SSA Payments Transferred to the State Supported Employment Services Program' ,
    '27. Unexpended Program Income' ,
    '28. Total Non-Federal Share of Allowable Expenditures (1st - 4th Qtr.)' ,
    '29. Non-Federal Share of Allowable Unliquidated Obligations (1st - 4th Qtr.)' ,
    '30. Non-Federal Share for Establishment of Facilities for CRP Purposes (1st - 4th Qtr.)' ,
    '31. Non-Federal Share for Construction of Facilities for CRP Purposes (1st - 4th Qtr.)' ,
    '32. Non-Federal Expenditures for Allowable Unliquidated Obligations Reported on the 4th Quarter Report, Line 29, Liquidated After the 4th Quarter (5th - 8th Qtr.)',
    '33. Additional New Non-Federal Expenditures (5th - 8th Qtr.)',
    '34. Non-Federal Share for Establishment of Facilities for CRP Purposes (5th - 8th Qtr.)',
    '35. Non-Federal Share for Construction of Facilities for CRP Purposes (5th - 8th Qtr.)',
    '36. Federal Cognizant Agency for Indirect Costs',
    'R1-a. Type',
    'R1-b. Rate',
    'R1-c. Period From',
    'R1-d. Period To',
    'R1-e. Base',
    'R1-f. Amount Charged',
    'R1-g. Federal Share',
    'R2-a. Type',
    'R2-b. Rate',
    'R2-c. Period From',
    'R2-d. Period To',
    'R2-e. Base',
    'R2-f. Amount Charged',
    'R2-g. Federal Share',
    'R3-a. Type',
    'R3-b. Rate',
    'R3-c. Period From',
    'R3-d. Period To',
    'R3-e. Base',
    'R3-f. Amount Charged',
    'R3-g. Federal Share',
    'Total for line e',
    'Total for line f',
    'Total for line g',
    '37. Administrative Expenditures',
    '38. Expenditures Incurred for the Provision of Pre-employment Transition Services by Agency Staff Only',
    '38-a. Required and Coordination Pre-employment Transition Services Provided by Agency Staff Only' ,
    '38-b. Authorized Pre-employment Transition Services Provided by Agency Staff Only',
    '39. Services to Groups',
    '39-a. Establishment, Development, or Improvement of CRP',
    '39-b. Telecommunication Systems',
    '39-c. Special Services to Provide Nonvisual Access to Information',
    '39-d. Technical Assistance to Businesses',
    '39-e. Business Enterprise Program (Randolph-Sheppard Program)',
    '39-f. Transition Consultation and Technical Assistance',
    '39-g. Transition Services to Youth and Students',
    '39-h. Establishment, Development, or Improvement of Assistive Technology',
    '39-i. Support for Advanced Training',
    '40. American Job Center Infrastructure Expenditures',
    '41. Total Innovation and Expansion (I&E) Expenditures',
    '41-a. I&E Expenditures Supporting State Rehabilitation Council Resource Plan',
    '41-b. I&E Expenditures Supporting Statewide Independent Living Council Resource Plan',
    '42. Remarks text',
    '43. Name of Authorized Certifying Official',
    '43. Title of Authorized Certifying Official',
    '44. Telephone (Area code, number, format: (999) 999-9999)',
    '44. Telephone Extension if any',
    '45. Email Address',
    '46. Signature of Authorized Certifying Official',
    'Date Report Submitted:',
]

parsedSummaryData = ''

def parseDetailsAndPutInSeparateExcel(url):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    tables = soup.find_all('table')
    records = []
    rowCount = 1
    for table in tables:
        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')
            col = 0
            data = [rowCount]
            for cell in cells:
                if col == 3:
                    data.append(f"https://rsa.ed.gov{cell.find('a')['href']}")
                else:
                    data.append(cell.text)
                col += 1
            records.append(data)
            rowCount += 1
    records

    return pd.DataFrame(records, columns=['Index', 'Submitting Organization',
                                        'Grant Award Number', 'Report though',
                                        'Link'])


def extractTextFromDiv(div):
    if div.find('span'):
        return div.find('span').text.strip()
    elif div.find('a'):
        return div.find('a').text.strip()
    else:
        return div.find_all(string=True)[2].strip()


def fetchTextFromFieldset(fieldset, dataDict, key, onlyChildren=False):
    if onlyChildren:
        for el in fieldset.find('div').children:
            if (el.name == 'div'):
                dataDict[key].append(extractTextFromDiv(el))
    else:
        for div in fieldset.find('div').find_all('div'):
            dataDict[key].append(extractTextFromDiv(div))


def extractAndSetFormattedData(details, fieldsets):
    data = {
        'common': [],
        'setA': [],
        'setB': [],
        'setC': [],
        'setD': [],
        'setE': [],
        'setF': {
            'data': [],
            'row1': [],
            'row2': [],
            'row3': [],
        },
        'setG': {
            'data': [],
            '38data': [],
            '39data': [],
            '41data': [],
        },
        'setH': [],
        'setI': {
            'data': [],
            '43data': [],
        },
        'ombNotice': [],
    }

    fetchTextFromFieldset(details, data, 'common', True)
    fetchTextFromFieldset(fieldsets[0], data, 'setA')
    fetchTextFromFieldset(fieldsets[1], data, 'setB')
    fetchTextFromFieldset(fieldsets[2], data, 'setC')
    fetchTextFromFieldset(fieldsets[3], data, 'setD')
    fetchTextFromFieldset(fieldsets[4], data, 'setE')
    fetchTextFromFieldset(fieldsets[5], data['setF'], 'data', True)
    fetchTextFromFieldset(fieldsets[6], data['setF'], 'row1')
    fetchTextFromFieldset(fieldsets[7], data['setF'], 'row2')
    fetchTextFromFieldset(fieldsets[8], data['setF'], 'row3')
    fetchTextFromFieldset(fieldsets[9], data['setG'], 'data', True)
    fetchTextFromFieldset(fieldsets[10], data['setG'], '38data')
    fetchTextFromFieldset(fieldsets[11], data['setG'], '39data')
    fetchTextFromFieldset(fieldsets[12], data['setG'], '41data')
    fetchTextFromFieldset(fieldsets[13], data, 'setH')
    fetchTextFromFieldset(fieldsets[14], data['setI'], 'data', True)

    if len(fieldsets) == 16:
        data['ombNotice'].append(fieldsets[15].find_all('div')[0].text.strip())
    else:
        fetchTextFromFieldset(fieldsets[15], data['setI'], '43data')
        data['ombNotice'].append(fieldsets[16].find_all('div')[0].text.strip())

    return data


def getEverythingInARow(data):
    rowData = []
    rowData = rowData + data['common']
    rowData = rowData + data['setA']
    rowData = rowData + data['setB']
    rowData = rowData + data['setC']
    rowData = rowData + data['setD']
    rowData = rowData + data['setE']
    rowData = rowData + [data['setF']['data'][0]]
    rowData = rowData + data['setF']['row1']
    rowData = rowData + data['setF']['row2']
    rowData = rowData + data['setF']['row3']
    rowData = rowData + data['setF']['data'][1:]
    rowData = rowData + data['setG']['data'][0:2]
    rowData = rowData + data['setG']['38data']
    rowData = rowData + data['setG']['data'][2:3]
    rowData = rowData + data['setG']['39data']
    rowData = rowData + data['setG']['data'][3:5]
    rowData = rowData + data['setG']['41data']
    rowData = rowData + data['setH']

    if len(data['setI']['43data']) > 0:
        rowData = rowData + data['setI']['43data']
        rowData = rowData + data['setI']['data']
        rowData = rowData + ['']
    else:
        rowData = rowData + data['setI']['data'][0:1]
        rowData = rowData + ['']
        rowData = rowData + data['setI']['data'][1:]

    return rowData


def extractAndSetFormattedDataForFY2024(details, fieldsets):
    data = {
        'common': [],
        'setA': [],
        'setB': {
            'data': [],
            '15data': [],
        },
        'setC': [],
        'setD': [],
        'setE': [],
        'setF': {
            'data': [],
            'row1': [],
            'row2': [],
            'row3': [],
        },
        'setG': {
            'data': [],
            '38data': [],
            '39data': [],
            '41data': [],
        },
        'setH': [],
        'setI': {
            'data': [],
            '43data': [],
        },
        'ombNotice': [],
    }

    fetchTextFromFieldset(details, data, 'common', True)
    fetchTextFromFieldset(fieldsets[0], data, 'setA')
    fetchTextFromFieldset(fieldsets[1], data['setB'], 'data', True)
    fetchTextFromFieldset(fieldsets[2], data['setB'], '15data')
    fetchTextFromFieldset(fieldsets[3], data, 'setC')
    fetchTextFromFieldset(fieldsets[4], data, 'setD')
    fetchTextFromFieldset(fieldsets[5], data, 'setE')
    fetchTextFromFieldset(fieldsets[6], data['setF'], 'data', True)
    fetchTextFromFieldset(fieldsets[7], data['setF'], 'row1')
    fetchTextFromFieldset(fieldsets[8], data['setF'], 'row2')
    fetchTextFromFieldset(fieldsets[9], data['setF'], 'row3')
    fetchTextFromFieldset(fieldsets[10], data['setG'], 'data', True)
    fetchTextFromFieldset(fieldsets[11], data['setG'], '38data')
    fetchTextFromFieldset(fieldsets[12], data['setG'], '39data')
    fetchTextFromFieldset(fieldsets[13], data['setG'], '41data')
    fetchTextFromFieldset(fieldsets[14], data, 'setH')
    fetchTextFromFieldset(fieldsets[15], data['setI'], 'data', True)
    fetchTextFromFieldset(fieldsets[16], data['setI'], '43data')
    data['ombNotice'].append(fieldsets[17].find_all('div')[0].text.strip())

    return data


def getEverythingInARowForFY2024(data):
    rowData = []
    rowData = rowData + data['common']
    rowData = rowData + data['setA']
    rowData = rowData + data['setB']['data'][0:6]
    rowData = rowData + data['setB']['15data']
    rowData = rowData + data['setB']['data'][6:]
    rowData = rowData + data['setC']
    rowData = rowData + data['setD']
    rowData = rowData + data['setE']
    rowData = rowData + [data['setF']['data'][0]]
    rowData = rowData + data['setF']['row1']
    rowData = rowData + data['setF']['row2']
    rowData = rowData + data['setF']['row3']
    rowData = rowData + data['setF']['data'][1:]
    rowData = rowData + data['setG']['data'][0:2]
    rowData = rowData + data['setG']['38data']
    rowData = rowData + data['setG']['data'][2:3]
    rowData = rowData + data['setG']['39data']
    rowData = rowData + data['setG']['data'][3:5]
    rowData = rowData + data['setG']['41data']
    rowData = rowData + data['setH']

    if len(data['setI']['43data']) > 0:
        rowData = rowData + data['setI']['43data']
        rowData = rowData + data['setI']['data']
        rowData = rowData + ['']
    else:
        rowData = rowData + data['setI']['data'][0:1]
        rowData = rowData + ['']
        rowData = rowData + data['setI']['data'][1:]

    return rowData


async def fetch_url(url, headers, isForFY2024=False):
    try:
        async with ClientSession(
                connector=aiohttp.TCPConnector(ssl=False)) as session:
            async with session.get(url, headers=headers) as response:
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                fieldsets = soup.find_all('fieldset')
                details = soup.find_all('details')
                if details:
                    details_data = details[0]
                    if (isForFY2024):
                        data = extractAndSetFormattedDataForFY2024(details_data,
                                                                   fieldsets)
                        formatted_data = getEverythingInARowForFY2024(data)
                    else:
                        data = extractAndSetFormattedData(details_data,
                                                          fieldsets)
                        formatted_data = getEverythingInARow(data)

                    custom_data = [url]
                    custom_data = custom_data + formatted_data[0:]
                    return custom_data
                else:
                    print(f"No 'details' element found on {url}")
                    return None
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return None


async def process_links(urls, headers, isForFY2024=False):
    tasks = [fetch_url(url, headers, isForFY2024) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [result for result in results if
            result is not None and result != Exception]


def get_event_loop():
    """
    Helper function to get or create an asyncio event loop.
    """
    try:
        loop = asyncio.get_running_loop()
        return loop
    except RuntimeError:
        return asyncio.new_event_loop()

st.title("RSA Webscrapping Tool")
st.markdown(
    """ 
    This is a tool to extract data from a give site: https://rsa.ed.gov/data/view-submission-rsa-17.  
    """
)

options = {
    2021 : 4,
    2022 : 3,
    2023 : 2,
    2024 : 1,
}

option = st.selectbox(
    'Which financial year do you like to extract data?',
     [
         2021,
         2022,
         2023,
         2024
     ])

'You selected: ', option

if st.button("Fetch Data"):
    url = f"https://rsa.ed.gov/data/view-submission-rsa-17?webform_submission_value={options[option]}&webform_submission_value_1=All"
    parsedSummaryData = parseDetailsAndPutInSeparateExcel(url)
    st.dataframe(data=parsedSummaryData)

    data = []
    limit = 150

    loop = get_event_loop()
    try:
        # Ensure the loop is running
        asyncio.set_event_loop(loop)
        isForFy2024 = option == 2024

        for i in range(0, math.ceil(len(parsedSummaryData['Link']) / limit)):
            formatted_data_list = loop.run_until_complete(
                process_links(parsedSummaryData['Link'][i:((i + 1) * limit)], headers, isForFy2024))
            data = data + formatted_data_list

        test_df = pd.DataFrame(data)

        if isForFy2024:
            fy2024FileColumns = column_header[0:21] + [
                '15.a. Required and Coordination Pre-employment Transition Service Activities and Other VR Services that Support Access to and Participation in Pre-Employment Transition Services',
                '15.b. Authorized Pre-employment Transition Service Activities'] + column_header[21:]

            test_df.columns = fy2024FileColumns
        else:
            test_df.columns = column_header

        unique_df = test_df.drop_duplicates()
        st.dataframe(data=unique_df)
    finally:
        loop.close()