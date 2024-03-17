import stocksonthego as stocks

"""
    Pobieranie notowań i nadawanie rankingu w oparciu o siłę relatywną, zapisywanie w google sheets z użyciem Oauth
"""

# funkcja użytkowa, przetwarza dane norgate i aktualizacje
def concat_csv():
    import os
    import glob
    import pandas as pd

    new_data_path = "C:\\Trading Data\\Stocks\\NDExport\\US Equities Delisted"
    old_data_path = "C:\\Trading Data\\Stocks\\US_Text\\Delisted Securities"
    combined_path = "C:\\Trading Data\\US Equities Delisted"

#    new_data_path = "C:\\Trading Data\\Stocks\\NDExport\\"
#    old_data_path = "C:\\Trading Data\\Stocks\\US_Text\\Indices\\S&P"
#    combined_path = "C:\\Trading Data\\Indices"

    os.chdir(old_data_path)

    all_filenames = [i for i in glob.glob('*.{}'.format('csv'))]
    #all_filenames = ['$SPX.csv'] #['ABDC-202001.csv']
    for f in all_filenames:
        try:
            #najpierw probuj czytac czy jest update wogle
            df_dest = pd.read_csv(old_data_path + "\\" + f)
            #potem czytaj to co juz mamy
            df_src = pd.read_csv(new_data_path+"\\"+f)

            df_dest.rename(columns={"Ticker":"Symbol"}, inplace=True)
            # sklej to
            combined_csv = pd.concat([df_dest, df_src])
            # i zapisz
            combined_csv.to_csv(combined_path+"\\"+f, index=False)
            print("done: "+f)
        except BaseException as e:
            yyyy=0
            print(str(e)+' ... ' + f)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #concat_csv()

    ## zawsze KONIECZNIE najpierw uruchom TEST, bo może być nieaktualna autoryzacja GOOGLE OAUTH

    # czyli trzerba usunąć plik:
    # del C:\Users\Legion\Dropbox\in\program\stocksontherun_py\sheets.googleapis.com-python*

    stocks.pobierz_yahoo_bulk(test=False, wczoraj=False)

    #stocks.pobierz_yahoo_bulk(test=False, wczoraj=False)

#    sotm.unikalne()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
