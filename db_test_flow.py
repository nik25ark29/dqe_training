import pyodbc
import pytest


class DBConnection:
    def __init__(self, driver, server, db_name):
        self.driver = driver
        self.server = server
        self.db_name = db_name
        with pyodbc.connect('Driver={' + f'{self.driver}' + '};'
                            + 'Server=' + f'{self.server}' + ';'
                            + f'Database={db_name}') as self.connection:
            self.cursor = self.connection.cursor()

    def close_connection(self):
        self.connection.close()


# Production.UnitMeasure
# check if UnitMeasureCode has only upper case letters (for example by requirements they all must be capitalized)
def upper_letters_in_unit_measure_code():
    conn = DBConnection('SQL Server', 'EPBYBREW0087', 'AdventureWorks2012')
    test_query = \
        '''
        select
            count(UnitMeasureCode) as num_of_rows_with_lower_letters
            from Production.UnitMeasure
            where UnitMeasureCode != upper(trim(UnitMeasureCode)) collate Latin1_General_BIN --case sensitive collation;
        '''
    conn.cursor.execute(test_query)
    result = conn.cursor.fetchall()[0][0]
    conn.close_connection()
    return result


def test_upper_letters_in_unit_measure_code():
    assert upper_letters_in_unit_measure_code() == 0
# ER: 0
# AR: 0


# check is UnitMeasureCode consists only of numbers
# (for example by requirements they can be represented but it can`t contain only numbers)
def digits_only_in_unit_measure_code():
    conn = DBConnection('SQL Server', 'EPBYBREW0087', 'AdventureWorks2012')
    test_query = \
        '''
        select
            count(try_cast(UnitMeasureCode as float)) as number_of_rows_with_digits_only
            from Production.UnitMeasure
            where UnitMeasureCode is not null --if string contains letters it won`t be converted and result will be null;
        '''
    conn.cursor.execute(test_query)
    result = conn.cursor.fetchall()[0][0]
    conn.close_connection()
    return result


def test_digits_only_in_unit_measure_code():
    assert digits_only_in_unit_measure_code() == 0
# ER: 0
# AR: 0


# Person.[Address]
# check for duplicated rows
def duplicates_in_person_address():
    conn = DBConnection('SQL Server', 'EPBYBREW0087', 'AdventureWorks2012')
    test_query = \
        '''
        with 
            dataset as 
            (
            select
                StateProvinceID, --each province may have cities with the same name
                City,
                PostalCode, --in each city multiple addresses may belong to one postal code
                AddressLine1,
                count(AddressID) as cnt
                from Person.[Address]
                group by StateProvinceID, City, PostalCode, AddressLine1
                having count(AddressID) > 1
            )
            select
                count(*) - (select count(*) from dataset) as num_of_duplicates --from all rows subtract unique rows to see number of redundant rows
                from Person.[Address]
                where concat(StateProvinceID, City, PostalCode, AddressLine1) in (select concat(StateProvinceID, City, PostalCode, AddressLine1) from dataset);
        '''
    conn.cursor.execute(test_query)
    result = conn.cursor.fetchall()[0][0]
    conn.close_connection()
    return result


def test_duplicates_in_person_address():
    assert duplicates_in_person_address() == 0
# ER: 0
# AR: 3


# check nulls where they are not supposed to be
def nulls_in_person_address():
    conn = DBConnection('SQL Server', 'EPBYBREW0087', 'AdventureWorks2012')
    test_query = \
        '''
        select
            count(*) as num_of_rows_with_nulls
            from Person.[Address]
            where AddressLine1 is null
                or City is null
                or StateProvinceID is null
                or PostalCode is null
                or rowguid is null
                or ModifiedDate is null;--all columns are not null due to table metadata
        '''
    conn.cursor.execute(test_query)
    result = conn.cursor.fetchall()[0][0]
    conn.close_connection()
    return result


def test_nulls_in_person_address():
    assert nulls_in_person_address() == 0
# ER: 0
# AR: 0


# Production.Document
# check if there are not missing levels in hierarchy and no extra branches
def missing_hierarchy_in_production_document():
    conn = DBConnection('SQL Server', 'EPBYBREW0087', 'AdventureWorks2012')
    test_query = \
        '''
        with
            possible_second_level_groups as
            (
            select distinct 
                cast(DocumentNode as nvarchar(255)) as second_level --get distinct 2 level group
                from Production.Document 
                where cast(DocumentNode as nvarchar(255)) like '/[1-9]/'
            )
            select 
                count(cast(DocumentNode as nvarchar(255))) as num_hierarchy_error_group
                from Production.Document
                where cast(DocumentNode as nvarchar(255)) != '/' --exclude first level
                    and substring(cast(DocumentNode as nvarchar(255)), 1, 3) not in (select second_level from possible_second_level_groups) --check existance of 3 level for non existing 2 level;
                    --for 3 lvl groups like /1/1/ or /2/1 or /3/5 check that their 2 lvl group which is represented by first section like /1/ or /2/ or /3/ exists 
        '''
    conn.cursor.execute(test_query)
    result = conn.cursor.fetchall()[0][0]
    conn.close_connection()
    return result


def test_missing_hierarchy_in_production_document():
    assert missing_hierarchy_in_production_document() == 0
# ER: 0
# AR: 0


# check if owner is correct
# check if groups on the same level with the same parent group have different Owner (must have the same)
def parent_on_third_level():
    conn = DBConnection('SQL Server', 'EPBYBREW0087', 'AdventureWorks2012')
    test_query = \
        '''
        select
            count(*) num_of_3_lvl_groups_with_different_owners
            from (
                select
                    substring(cast(DocumentNode as nvarchar(255)), 1, 3) as parent_lvl, --get distinct 2 level group
                    count(distinct Owner) as cnt_owners --count owners, each may have only one, because it`s a tree
                    from Production.Document
                    where DocumentLevel = 2
                    group by substring(cast(DocumentNode as nvarchar(255)), 1, 3) --by 2 level group
                    having count(distinct Owner) > 1
                ) as tab; 
        '''
    conn.cursor.execute(test_query)
    result = conn.cursor.fetchall()[0][0]
    conn.close_connection()
    return result


def test_parent_on_third_level():
    assert parent_on_third_level() == 0
# ER: 0
# AR: 2


# check if different 3 lvl groups have unique owners
def unique_owners_on_third_level():
    conn = DBConnection('SQL Server', 'EPBYBREW0087', 'AdventureWorks2012')
    test_query = \
        '''
        select
            /*case when each of 3 level groups has unique owner then number of owners must be equal to number of groups, if not there are errors in hierarchy*/
            case 
                when count(distinct substring(cast(DocumentNode as nvarchar(255)), 1, 3)) /*num_of_3_lvl_groups*/ 
                    = count(distinct Owner) /*num_of_3_lvl_owners*/
                then 0 --no error
                else 1 --'hierarchy violation' 
            end as hierarchy_violation
            from Production.Document
            where DocumentLevel = 2; 
        '''
    conn.cursor.execute(test_query)
    result = conn.cursor.fetchall()[0][0]
    conn.close_connection()
    return result


def test_unique_owners_on_third_level():
    assert unique_owners_on_third_level() == 0
# ER: 0 boolean logic (0/1)
# AR: 1
