import psycopg2
import os

def extract_rh_groundtruth():
    """
    Need to clean data still
    """
    conn = psycopg2.connect(database='geo2020_RHe_test', user='chris', password='hoikang3', host='localhost', port='5432')

    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute('''DROP TABLE IF EXISTS public.groundtruth_rh;
    CREATE TABLE public.groundtruth_rh AS
    SELECT cityobject.id, cityobject.objectclass_id, cityobject.gmlid, cityobject.name, cityobject_genericattrib.strval AS building_type
    FROM citydb.cityobject, citydb.cityobject_genericattrib
    WHERE cityobject.id = cityobject_genericattrib.cityobject_id AND cityobject_genericattrib.attrname = 'building_type'
    ORDER BY id ASC;''')
    
    os.system('pg_dump -t public.groundtruth_rh geo2020_RHe_test | psql geo2020')

    cursor.execute("DROP TABLE IF EXISTS public.groundtruth_rh;")
    return

def extract_ep_groundtruth():
    """
    Need to format pand id
    """
    conn = psycopg2.connect(database='geo2020', user='chris', password='hoikang3', host='localhost', port='5432')

    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute('''DROP TABLE IF EXISTS public.groundtruth_ep;
    CREATE TABLE public.groundtruth_ep AS
    SELECT index, "Pand_bagpandid", "Pand_gebouwtype"
    FROM public."ep-online"
    WHERE "Pand_status" = 'Bestaand' AND "Pand_gebouwtype" IS NOT NULL AND "Pand_bagpandid" IS NOT NULL 
    ORDER BY index ASC;''')
    return

def main():
    #extract_rh_groundtruth()
    extract_ep_groundtruth()

if __name__ == '__main__':
    main()