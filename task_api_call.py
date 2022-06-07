# WRITTEN BY ATHUL NANDASWAROOP
# A PROGRAM TO BULK CALL PAGINATEG APIS CONCURRENTLY USING MUTITHREADING ALONG WITH 
# A COMPARISON VS SINGLE THREADED EXECUTION MODE BY COMPARING EXECUTION TIME OF BOTH

from concurrent.futures import ThreadPoolExecutor, wait
import requests
import time


BASE_URL: str = 'https://jsonmock.hackerrank.com/api/transactions/search'

workers: int = 20 # MAX THREADS LIMIT


# def setParams(page, txnType): return {'txnType': txnType, 'page': page}# pep8 SUGGEST THIS SYNTAX OVER lambdas
setParams = lambda page, txnType : {'txnType': txnType, 'page': page} #SETTING PATH PARAMS OF API REQUEST


def fetch(session, url, params): # TO FETCH API
    with session.get(url, params=params) as response:
        return response.json()


def preFetch(executor, session): # TO GET TOTAL NUMBER OF PAGES TO IDENTIFY NUMBER OF TOTAL REQUESTS TO BE PERFORMED

    cred = executor.submit(fetch, session, BASE_URL,
                           setParams(txnType='CREDIT', page=1))
    deb = executor.submit(fetch, session, BASE_URL,
                          setParams(txnType='DEBIT', page=1))
    global credit_pages
    global debit_pages
    credit_pages = cred.result()['total_pages']
    debit_pages = deb.result()['total_pages']

    wait((cred, deb))


def main(): # MAIN FUNCTIONALITY IMPLEMENTATION

    with ThreadPoolExecutor(max_workers=workers) as executor:

        with requests.Session() as session:

            preFetch(executor, session)
            print("credit_pages : ", credit_pages)
            print("debit_pages : ", debit_pages)

            credits = []
            debits = []

            loc_amounts = {}
            usr_amounts = {}

            for item in executor.map(fetch, [session] * credit_pages, [BASE_URL] * credit_pages, [setParams(txnType='CREDIT', page=i) for i in range(1, credit_pages+1)]):

                credits += item['data']

            for item in executor.map(fetch, [session] * debit_pages, [BASE_URL] * debit_pages, [setParams(txnType='DEBIT', page=i) for i in range(1, debit_pages+1)]):

                debits += item['data']

            executor.shutdown(wait=False)

            print("total credits : ", len(credits))
            print("total debits : ", len(debits))
            for c in credits:

                key_loc = str(c['location']['id'])
                key_usr = str(c['userId'])

                if(key_loc not in loc_amounts):
                    loc_amounts[key_loc] = []
                if(key_usr not in usr_amounts):
                    usr_amounts[key_usr] = []

                amount = int(float(c['amount'][1:].replace(',', '')))
                loc_amounts[key_loc].append(amount)
                usr_amounts[key_usr].append(amount)

            for d in debits:

                key_loc = str(d['location']['id'])
                key_usr = str(d['userId'])

                if(key_loc not in loc_amounts):
                    loc_amounts[key_loc] = []
                if(key_usr not in usr_amounts):
                    usr_amounts[key_usr] = []

                amount = -1*int(float(d['amount'][1:].replace(',', '')))
                loc_amounts[key_loc].append(amount)
                usr_amounts[key_usr].append(amount)

            print("\nLocation id based Total Amount (total credit - total debit)")
            for key, value in loc_amounts.items():
                print(f"location id : {key} ->  total : {sum(value)}")

            print("\nUser id based Total Amount (total credit - total debit)")
            for key, value in usr_amounts.items():
                print(f"user id : {key} ->  total : {sum(value)}")


start_time = time.time()

main()

mtime = time.time() - start_time

print(
    f"\nMulti Threaded Code took just    { mtime }  seconds to complete everything including {credit_pages+debit_pages} api calls  ")
print("\nWaiting for Single threaded mode's results  . . .")

sync_time = time.time()
for i in range(0, credit_pages+debit_pages): # CALLING APIS IN SINGLE THREADED MODE
    requests.get(BASE_URL)

stime = time.time()-sync_time
print(
    f"\nSynchronous/Single threaded Code took huge    {stime}  seconds  to just to complete {credit_pages+debit_pages} api calls ")

print(
    f"\nOur Code (Multi Threaded) is more than {stime/mtime} times faster than single threaded execution")
