import os
import glob
import csv

class DataPrep():
    """
    A class used to prepare a csv file to be used in the ETL pipeline
    
    . . .
    
    Methods
    -------
    _collect_files() 
        collect and join the file path and roots with the subdirectories
    _extract_data()
        create a list of rows that will be generated from each file
    write_csv()
        create a smaller csv file that will be used in the Apache Cassandra tables
    """
    def __init__(self, filepath_in, filepath_out):
        """
        Parameters
        ----------
        filepath_in : str
            The path to the original csv file
        filepath_out : str
            The path to save the processed csv file
        """
        self.filepath_in = filepath_in
        self.filepath_out = filepath_out
    
    def _collect_files(self):
        """Collect and join files in the subdirectories

        Returns
        -------
        list
            list of files 
        """
        for root, dirs, files in os.walk(self.filepath_in):
            file_path_list = glob.glob(os.path.join(root,'*'))
        
        return file_path_list
    
    def _extract_data(self):
        """Extract data by row from _collect_files and append in a list        

        Returns
        -------
        list
            list of files 
        """
        full_data_rows_list = []
        file_path_list = self._collect_files()
        for f in file_path_list:
            with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
                csvreader = csv.reader(csvfile) 
                next(csvreader)
                for line in csvreader:
                    full_data_rows_list.append(line) 
        
        return full_data_rows_list
    
    def write_csv(self):
        """Create a new csv file smaller that will be used with Apache Cassandra 
        
        Returns
        -------
        none
            
        """
        full_data_rows_list = self._extract_data()
        csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        with open(self.filepath_out + '.csv', 'w', encoding = 'utf8', newline='') as f:
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                        'level','location','sessionId','song','userId'])
            for row in full_data_rows_list:
                if (row[0] == ''):
                    continue
                writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], 
                                 row[7], row[8], row[12], row[13], row[16]))

