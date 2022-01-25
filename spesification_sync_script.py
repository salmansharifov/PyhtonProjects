
import psycopg2 as psycopg2
import pandas as pandas

sqlConnection = None
try:
    fileDir = r'C:\Users\SalmanSharifov\Downloads\Məşğulluq təsnifatı_Qruplaşma.xlsx'
    sqlConnection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="admin")

    excelData = pandas.read_excel(fileDir,
                                  sheet_name=0)
    fileData = excelData.values.tolist()
    professions = {}
    specs = {}
    for data in fileData:
        professions.update({data[4].replace("_x000D_\n", ""): data[6]})
        specs.update({data[9]: data[8]})

    with sqlConnection.cursor() as cursor:
        for key, value in professions.items():
            queryForProfessions = """ UPDATE professions SET name = %s WHERE classification_code = %s """
            params = (value, key,)
            cursor.execute(queryForProfessions, params)
            rc = cursor.statusmessage
            sqlConnection.commit()
        for key, value in specs.items():
            if pandas.isna(value) is True:
                try:
                    queryForProfessionSpecDelete = "DELETE FROM profession_specifications WHERE specification_id = %s"
                    cursor.execute(queryForProfessionSpecDelete, (key,))
                    queryForSpecDelete = "DELETE FROM specifications  WHERE id = %s"
                    cursor.execute(queryForSpecDelete, (key,))
                except Exception as e:
                    print('Exception in spec delete: ', e)
            else:
                try:
                    queryForSpecUpdate = "UPDATE specifications SET name = %s WHERE id = %s"
                    cursor.execute(queryForSpecUpdate, (value, key,))
                except Exception as e:
                    print('Exception in spec update: ', e)
            sqlConnection.commit()

    sqlConnection.close()
    print("ALL IS WELL")
except Exception as e:
    print(e)
finally:
    if sqlConnection is not None:
        sqlConnection.close()
        print('Database connection closed.')
