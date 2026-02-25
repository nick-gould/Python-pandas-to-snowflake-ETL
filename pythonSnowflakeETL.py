# requires snowflake python connector, can be modified for other databases

class UpdateSnowflake:
    def __init__(self, Connection, Database, Schema, Table = '', DataFrame=None):
        try:
          self.df = DataFrame.copy(deep=True)
        except:
          self.df = DataFrame
        self.table = Table
        self.wip = Table + '_wip'
        self.conn = Connection
        self.db = Database
        self.schema = Schema
        
    def CreateTable(self, TableSQL):
        self.tableSQL = TableSQL
        executeMySQL(f'''
        create table if not exists {self.db}.{self.schema}.{self.table} (   
        {TableSQL}
        );
        ''', con=conn)
        
    def CreateWIP(self, wipSQL):
        self.wipSQL = wipSQL
        executeMySQL(f'''
        create or replace table {self.db}.{self.schema}.{self.wip}(   
        {wipSQL}
        );
        ''', con=conn)
        
        executeMySQL(f'''
        GRANT SELECT ON {self.db}.{self.schema}.{self.wip} TO ROLE reader
        ''',  con=conn)
        
    def BuildWIP(self):
        WritePandasOutput = write_pandas(df=self.df, conn=self.conn, database=self.db, schema=self.schema, table_name=self.wip, quote_identifiers=False)
        
    def SwapWIP(self):
        executeMySQL(f'''
        ALTER TABLE IF EXISTS {self.db}.{self.schema}.{self.table} 
        SWAP WITH {self.db}.{self.schema}.{self.wip}
        ''', con=conn)
        
        executeMySQL(f'''
        GRANT SELECT ON {self.db}.{self.schema}.{self.table} TO ROLE reader
        ''', con=conn)
        
    def DropWIP(self):
        executeMySQL(f'''
        DROP TABLE {self.db}.{self.schema}.{self.wip}
        ''', con=conn)
        
    def addTimeStampToDataframe(self):
        timestampQuery = 'select current_timestamp()::TIMESTAMP_LTZ as load_timestamp'

        myTimestamp = pd.read_sql(timestampQuery, conn)
        myTimestamp['LOAD_TIMESTAMP'][0]
        self.df['load_timestamp'] = myTimestamp['LOAD_TIMESTAMP'][0]
        self.df['modified_timestamp'] = myTimestamp['LOAD_TIMESTAMP'][0]
        self.df['modified_by'] = 'https://github.com/nick-gould'
        
    def appendDataFrameToExistingTable(self):
        self.existingDataQuery = f'''select * from {self.db}.{self.schema}.{self.table}'''
        self.df_existingData = pd.read_sql(self.existingDataQuery, self.conn)
        self.df_existingData.columns = self.df_existingData.columns.str.lower()
        self.df.columns = self.df.columns.str.lower()
        self.df = self.df.append(self.df_existingData)
        
    def MaterializeView(self, TableDefinitionSQL, viewQuery, comments, addComments=True):
        self.TableDefinitionSQL = TableDefinitionSQL
        self.viewQuery = viewQuery
        
        executeMySQL(f'''
        create table if not exists {self.db}.{self.schema}.{self.table}_t (   
        {TableDefinitionSQL}
        );
        ''', con=conn)
            
        executeMySQL(f'''
        create or replace view {self.db}.{self.schema}.{self.table}_v as   
        {viewQuery}
        ;
        ''', con=conn)
        
        executeMySQL(f'''
        GRANT SELECT ON {self.db}.{self.schema}.{self.table}_v TO ROLE reader
        ''',  con=conn)
        
        executeMySQL(f'''
        create or replace table {self.db}.{self.schema}.{self.wip}_t as   
        select * from {self.db}.{self.schema}.{self.table}_v
        ;
        ''', con=conn)
        
        if addComments == True:
            executeMySQL(f'''
            ALTER TABLE IF EXISTS {self.db}.{self.schema}.{self.table}_t
            ALTER COLUMN {comments}
        ''', con=conn)

        else:
            pass
        
        executeMySQL(f'''
        GRANT SELECT ON {self.db}.{self.schema}.{self.wip}_t TO ROLE reader
        ''',  con=conn)
        
        executeMySQL(f'''
        ALTER TABLE IF EXISTS {self.db}.{self.schema}.{self.table}_t
        SWAP WITH {self.db}.{self.schema}.{self.wip}_t
        ''', con=conn)
        
        executeMySQL(f'''
        GRANT SELECT ON {self.db}.{self.schema}.{self.table}_t TO ROLE reader
        ''', con=conn)        
        
        executeMySQL(f'''
        DROP TABLE {self.db}.{self.schema}.{self.wip}_t
        ''', con=conn)
        
    def getNextSequenceNumber(self, sequence):
        self.sequence = sequence
        sequenceQuery = f'select {self.db}.{self.schema}.{sequence}.nextval as sequence_id'

        mySequence = pd.read_sql(sequenceQuery, conn)
        self.sequence_id = mySequence['SEQUENCE_ID'][0]
        
    def addSequenceID(self, sequence_name, id):
        self.df[sequence_name] = id
        
def executeMySQL(someSQL, con):
    foo = con.cursor()
    try:
        foo.execute(someSQL)
    except Exception as e:
        print(e)
        raise
    foo.close()


# ~~~~~~~~~~~~~~~~~~~~~
# Edit the next three variables
# ~~~~~~~~~~~~~~~~~~~~~

# The SQL code goes between the quotes
myTableSQL = f"""



"""

# need to define ThisTable name and the dataframe being written to SQL
ThisTable = ''
dfToWrite = ''

myTableWrite = UpdateSnowflake(Connection=conn, Database=myParameterizedDatabase, Schema=myParameterizedSchema, Table=ThisTable, DataFrame=dfToWrite)
myTableWrite.CreateTable(myTableSQL)
myTableWrite.CreateWIP(myTableSQL)
myTableWrite.addTimeStampToDataframe()
myTableWrite.addSequenceID(sequenceColName, mySequenceObject.sequence_id)
myTableWrite.appendDataFrameToExistingTable()
myTableWrite.BuildWIP()
myTableWrite.SwapWIP()
myTableWrite.DropWIP()