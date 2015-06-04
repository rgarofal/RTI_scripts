# coding=utf-8
__author__ = 'rgarofal'

import unittest
import re
import itertools

import cx_Oracle



#
# template select and update
#
# Il file di input di export deve essere formato in generale così:
#
# CREATE VIEW ERICSSON_MME_EPC.BSSGP_MEAS
# AS SELECT .....
# FROM ERICSSON_MME_EPC.BSSGP_Meas@OPTCRE01
# ;
#
# Attenzione che: "AS SELECT" sia in maiuscolo
# : non ci devono essere AS nel nome delle colonne
#



format_string_date = '\'DD-MON-YY\''
check_starttime_1 = 'STARTTIME'
check_starttime_2 = 'START_TIME'
type_obj_view = '\'VIEW\''

extract_min_day = 'SET VERIFY OFF;  select min(%s) from %s.%s;'
extract_sample_for_day = 'SET VERIFY OFF; select AVG(count(*)) from %s.%s group by TO_CHAR(%s, %s)'

create_synonym_after = 'CREATE OR REPLACE SYNONYM RTI.%s FOR LTE.%s'

extract_all_view_of_schema = 'select OBJECT_NAME from all_objects where OBJECT_TYPE = %s and owner = %s'
add_where_to_exclude = ' and OBJECT_name not in (%s)'

#
# Lista file con DDL da importare ... individuato con la stringa dello schema sorgente.
# Lista dei nomi degli schemi da esportare.
#
name_file_report_export = 'esportazione_%s.sql'
name_file_report_import = 'importazione_%s.sql'
namefile_report_log_import = 'report_importazione_%s.log'
namefile_stats_scripts_head = 'stats_for_importviews_%s.sql'
directory_base = 'D:\\rgarofal_DOCUMENT\\RTI_SPINDOX\\EXPORT\\'
directory_category = 'MAVENIR_DSC_DSR'
directory_export_ddl = (directory_base) + (directory_category)
report_created_view = 'REPORT_IMPORT_RM132.log'
report_statistics_view = 'REPORT_STATISTICS_VIEW_%s.sql'

# Old Code list_of_schema_to_import = ['ERICSSON_MME_EPC', 'HUAWEI_MME_EPC', 'ERICSSON_PGW_SGW_EPC', 'HUAWEI_PGW_SGW_EPC', 'HUAWEI_CG', 'ALU_SCP_PREPAID_DRN']

# Relazione fra SOTTOCATEGORIA e SCHEMA (sul foglio excel)
list_of_schema_to_import = {'ERICSSON_MME_EPC': 'LTE', 'HUAWEI_MME_EPC': 'LTE', 'ERICSSON_PGW_SGW_EPC': 'LTE',
                            'HUAWEI_PGW_SGW_EPC': 'LTE', 'HUAWEI_CG': 'LTE', 'ALU_SCP_PREPAID_DRN': 'SCP', 'MAVENIR_DSC_DSR': 'LTE'}
# Relazione fra CATEGORY e SCHEMA (foglio excel)
map_category_and_new_schema = {'LTE': 'LTE', 'SCP': 'SCP'}
map_schema_to_macro_category = {'LTE': 'CORE PS', 'SCP': 'IN'}

# list_of_schema_to_import = ['ERICSSON_MME_EPC']
# list_of_schema_to_import = ['HUAWEI_PGW_SGW_EPC']
filter_views = ['BSSGP_MEAS', 'GTP_MEAS', 'MM_MEAS', 'SM_MEAS', 'GB_MEAS', 'GB_MM_MEAS', 'GB_SM_MEAS', 'IU_MM_MEAS',
                'IU_SM_MEAS', 'GTPU_MEAS', 'PGW_APN_DATA_PLANE', 'GGSN_U_PIC', 'PGW_APN_BEARERS', 'SGW_SESSIONS',
                'PGW_FW_MEAS', 'SYS_RES_MEAS_CPU', 'S_PGW_SESSION_MEAS', 'S_PGW_FW_MEAS_APN']

# Relazione fra SOTTOCATEGORIA (foglio excel) e Prefisso Nuove Viste
mapping_schema = {'ERICSSON_MME_EPC': 'V_ERI_MME', 'HUAWEI_MME_EPC': 'V_HUA_MME', 'ERICSSON_PGW_SGW_EPC': 'V_ERI_PGW',
                  'HUAWEI_PGW_SGW_EPC': 'V_HUA_PGW', 'HUAWEI_CG': 'V_CDR', 'ALU_SCP_PREPAID_DRN': 'V_ALU', 'MAVENIR_DSC_DSR':'V_MAV'}

# Suffisso al nome della view per distinguerla. (vedi foglio excel _DRN è uguale alla SOTTOCATEGORIA)
mapping_schema_subcat = {'ERICSSON_MME_EPC': '', 'HUAWEI_MME_EPC': '', 'ERICSSON_PGW_SGW_EPC': '',
                         'HUAWEI_PGW_SGW_EPC': '', 'HUAWEI_CG': '', 'ALU_SCP_PREPAID_DRN': '_DRN',  'MAVENIR_DSC_DSR':''}
# Relazione fra Schema di OPTIMA e MACROCATEGORIA (vedi foglio excel)
mapping_macro_cat = {'ERICSSON_MME_EPC': 'CORE PS', 'HUAWEI_MME_EPC': 'CORE PS', 'ERICSSON_PGW_SGW_EPC': 'CORE PS',
                     'HUAWEI_PGW_SGW_EPC': 'CORE PS', 'HUAWEI_CG': 'CORE PS', 'ALU_SCP_PREPAID_DRN': 'IN', 'MAVENIR_DSC_DSR':'CORE PS'}

# Mapping fra nome view esteso e quella più compatta per non incorrere nell'eerrore del identificativo troppo lungo
mapping = {'V_ERI_PGW_GGSN_APN_FBC_SERVCLASS': 'V_ERI_PGW_GGSN_APN_FBC_SERVCLS',
           'V_ERI_PGW_GGSN_APN_SACC3_SERVID': 'V_ERI_PGW_GGSN_APN_SACC3_SERVD',
           'V_ERI_PGW_GGSN_U_PIC': 'V_ERI_GGSN_U_PIC',
           'V_ERI_PGW_PGW_APN_BEARERS': 'V_ERI_PGW_APN_BEARERS',
           'V_ERI_PGW_PGW_APN_DATA_PLANE': 'V_ERI_PGW_APN_DATA_PLANE',
           'V_ERI_PGW_SGW_SESSIONS': 'V_ERI_SGW_SESSIONS',
           'V_HUA_MME_S6A_INTERFACE_DIAMLINK': 'V_HUA_MME_S6A_INTERFACE_DIAMLK',
           'V_HUA_MME_S6A_INTERFACE_DIAMLSET': 'V_HUA_MME_S6A_INTERF_DIAMLSET',
           'V_HUA_PGW_GGSNROLE_TRASP_MEAS_TC': 'V_HUA_PGW_GGSNROLE_TRSP_MES_TC',
           'V_HUA_PGW_SGW_USERDATA_MEAS_APN': 'V_HUA_PGW_SGW_USERDTA_MEAS_APN',
           'V_HUA_PGW_S_PGW_SESSION_MEAS_APN': 'V_HUA_PGW_S_PGW_SESS_MEAS_APN',
           'V_HUA_PGW_S5_S8_PGWSIGNERR_MEAS': 'V_HUA_PGW_S5_S8_PGWSIGNERR_MES',
           'V_HUA_PGW_S5_S8_SGWSIGNERR_MEAS': 'V_HUA_PGW_S5_S8_SGWSIGNERR_MES'
}


# change PEP
start_patterns_str = 'CREATE'
end_patterns_str = ";"
# change PEP
start_patterns = 'CREATE'
end_patterns = ';'


def section_with_bounds(gen):
    section_in_play = False
    for line in gen:
        if line.startswith(start_patterns):
            section_in_play = True
        if section_in_play:
            yield line
        if line.endswith(end_patterns):
            section_in_play = False


# prefiltrare le stringhe con '
ddl = 'CREATE OR REPLACE FORCE VIEW "LTE"."V_HUA_SYS_RES_CPU_MEAS" ("DATA", "HOUR", "GW_NAME", "CPU_ID", "STARTTIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE") AS SELECT "DATA", "HOUR", "GW_NAME", "CPU_ID", "STARTTIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE" FROM (SELECT TRUNC(STARTTIME) DATA, TO_CHAR(STARTTIME, \'HH24:MI\') HOUR, "GW_NAME", "CPU_ID", "STARTTIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE" FROM HUAWEI_PGW_SGW_EPC.SYS_RES_MEAS_CPU\@opt) WHERE STARTTIME BETWEEN DATA AND DATA +0.99999'
dml_cat_tab = 'INSERT INTO CAT_TAB (NOME_TAB, CATEGORIA, ADMIN, REPORT, ALARM, COD_UTENTE_AGG, DT_AGG, EXPORT, DS_TARGET, SAMPLING) VALUES (\'%s\', \'%s\',\'N\',\'S\',\'S\',\'RGAROFALO\', SYSDATE, \'N\', \'RTI132\', 15);\n'
dml_query_tab = 'INSERT INTO TAB_QUERY (NOME_TAB, CATEGORIA, NOME_QUERY, MESSAGGIO, STATO, DATA_INIZIO, COD_UTENTE_CONFIG, HELP_FILE_NAME, TIPO_QUERY, NOME_AGGR) ' \
                '               VALUES (\'%s\', \'%s\', \'%s\',\'\',\'B\', SYSDATE, \'RGAROFALO\', \'\',\'C\',\'%s\');\n'
dml_query_fields = 'INSERT INTO FIELDS_QUERY(NOME_TAB,CATEGORIA, NOME_QUERY, NOME_FIELD, FIELD_ORDER, FIELD_TYPE, FIELD_LAYOUT, FIELD_LABEL, MUST_FILLED, ALIAS_LIST,CONSISTENZA, NOME_QUERY_WEB) ' \
                   'VALUES(\'%s\', \'%s\', \'%s\',\'DATA\',1,\'DATE\',\'\',\'Date\',\'N\',\'\',\'\', \'\');\n'
dml_query_checks_columns = 'select NOME_QUERY, count(*) from FIELDS_QUERY where nome_query =\'%s\' group by NOME_QUERY;\n'
dml_update_usage_columns = 'UPDATE FIELDS_QUERY SET MUST_FILLED = (\'Y\') WHERE NOME_FIELD IN ( %s) AND NOME_TAB = \'%s\';\n'
dml_insert_grants = 'INSERT INTO UTENTI_GRANTS_QUERY(COD_UTENTE, NOME_QUERY, NOME_TAB, CATEGORIA, COD_MCAT) ' \
                    'VALUES(\'%s\', \'%s\', \'%s\',\'%s\', \'%s\');\n'


#
# Unit Test
#


class UnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """

        :type cls: object
        """
        cls.ip_addr = '10.178.7.187'
        cls.sid = 'RTI.vodafone.com'
        cls.user = 'rti'
        cls.passwd = 'wrti'
        cls._connection_rmis132 = ora_connect(cls.ip_addr, cls.sid, cls.user, cls.passwd)

        cls.mapping_schema_test = {'ERICSSON_GGSN': 'V_ERI_GGSN', 'ERICSSON_MME_EPC': 'V_ERI_MME',
                                   'HUAWEI_MME_EPC': 'V_HUA_MME', 'ERICSSON_PGW_SGW_EPC': 'V_ERI_PGW',
                                   'HUAWEI_PGW_SGW_EPC': 'V_HUA_PGW'}

        cls.mapping_schema_subcat_test = {'ERICSSON_GGSN': '', 'ERICSSON_MME_EPC': '',
                                   'HUAWEI_MME_EPC': '', 'ERICSSON_PGW_SGW_EPC': '',
                                   'HUAWEI_PGW_SGW_EPC': ''}


        cls.ddl_old_test = ['CREATE VIEW ERICSSON_GGSN.APN_FBC_CCAS_STATS\n',
                            'AS SELECT    "GGSN_NAME", "CC_AS_ID", "APN_NAME", "STARTTIME", "ZONE",\n',
                            '                "DURATION", "INSERT_TIME", "GGSNAPNFBCCCASNAME",\n',
                            '                "GGSNAPNFBCCCASSTARTREQ", "GGSNAPNFBCCCASSTARTREQFAIL",\n',
                            '                "GGSNAPNFBCCCASUPDATEREQ", "GGSNAPNFBCCCASUPDATEREQFAIL",\n',
                            '                "GGSNAPNFBCCCASSTOPREQ", "GGSNAPNFBCCCASSTOPREQFAIL",\n',
                            '                "GGSNAPNFBCCCASUSERSERVICEDEN", "GGSNAPNFBCCCASUSERUNKNOWN",\n',
                            '                "GGSNAPNSACCCCASAUTHREJECT", "GGSNAPNSACCCCASCCNOTAPPL"\n',
                            '      FROM    ERICSSON_GGSN.APN_FBC_CCAS_STATS@OPTSSC\n']


    # @classmethod
    # def tearDown(cls):
    # if cls._connection_rmis132:
    #      cls._connection_rmis132.close()
    #       cls._connection_rmis132 = None




    #@unittest.skip("I know failed\n")
    def test_extract_ddl(self):
        print("Connessione e prova DDL  su RTI-132")
        ddl_string = extract_ddl_object('VIEW', 'V_GIS_RNG_TOPOLOGY', 'ABNCBN', self._connection_rmis132)
        result = ddl_string.fetchall()
        result_finale = result[0][0].read()
        result_finale = result_finale.replace('\n', '').strip().replace(" ", "")
        print "Risultato query = %s" % result_finale
        return_str = (
            'CREATE OR REPLACE FORCE VIEW "ABNCBN"."V_GIS_RNG_TOPOLOGY" ("ID_ZONA", "RING_CODE", "RING_STATUS", "ELEMENT_CODE", "ELEMENT_TYPE", "RING_TYPE")'
            'AS SELECT "ID_ZONA", "RING_CODE", "RING_STATUS", "ELEMENT_CODE", "ELEMENT_TYPE", "RING_TYPE" FROM rti.nis_gis_rng_topology@rtin57' )
        print "Risultato aspettato = %s" % return_str
        self.assertMultiLineEqual(result_finale, return_str.strip().replace(" ", ""))
        ddl_string.close()

    def test_connection_RTI_132(self):
        print("Connessione su RTI-132")
        ip_addr = '10.178.7.187'
        sid = 'RTI.vodafone.com'
        user = 'rti'
        passwd = 'wrti'
        conn = ora_connect(ip_addr, sid, user, passwd)
        self.assertIsNotNone(conn)
        conn.close()

    #@unittest.skip("I know failed\n")
    def test_connection_N57(self):
        # (RN75) conn = ora_connect('10.192.26.184','RTI.vodafone.com', 'rti', 'wrti')
        print("Connessione su RN_57")
        ip_addr = '10.192.26.184'
        sid = 'RTI.vodafone.com'
        user = 'rti'
        passwd = 'wrti'
        conn = None
        conn = ora_connect(ip_addr, sid, user, passwd)
        self.assertIsNotNone(conn)
        conn.close()

    def test_connection_OPTIMA(self):
        # (OPTIMA) conn = ora_connect('10.23.41.12','repdb', 'nwis', 'woptnis02')
        print("Connessione su OPTIMA")
        ip_addr = '10.23.41.12'
        sid = 'repdb'
        user = 'nwis'
        passwd = 'woptnis02'
        conn = ora_connect(ip_addr, sid, user, passwd)
        self.assertIsNotNone(conn)
        conn.close()

    def test_connections_negative(self):
        print("Connessione su RTI-132")
        ip_addr = '10.178.7.188'
        sid = 'RTI.vodafone.com'
        user = 'rti'
        passwd = 'wrti'
        try:
            conn = ora_connect(ip_addr, sid, user, passwd)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print "Connection on wrong IP checked"
            self.assertTrue(error.code == 12541)

    def test_extraction_of_starttime(self):
        ddl_test1 = 'CREATE OR REPLACE FORCE VIEW "LTE"."V_HUA_SYS_RES_CPU_MEAS" ("DATA", "HOUR", "GW_NAME", "CPU_ID", "STARTTIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE") AS SELECT "DATA", "HOUR", "GW_NAME", "CPU_ID", "STARTTIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE" FROM (SELECT TRUNC(STARTTIME) DATA, TO_CHAR(STARTTIME, \'HH24:MI\') HOUR, "GW_NAME", "CPU_ID", "STARTTIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE" FROM HUAWEI_PGW_SGW_EPC.SYS_RES_MEAS_CPU\@opt) WHERE STARTTIME BETWEEN DATA AND DATA +0.99999'
        test_value = _check_type_starttime(ddl_test1)
        msg = 'Errore ad estrarre la stringa %s sulla funzione %s' % (check_starttime_1, '_check_type_starttime')
        self.assertEqual(test_value, check_starttime_1, msg)

    def test_extraction_of_start_time(self):
        ddl_test2 = 'CREATE OR REPLACE FORCE VIEW "LTE"."V_HUA_SYS_RES_CPU_MEAS" ("DATA", "HOUR", "GW_NAME", "CPU_ID", "START_TIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE") AS SELECT "DATA", "HOUR", "GW_NAME", "CPU_ID", "START_TIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE" FROM (SELECT TRUNC(START_TIME) DATA, TO_CHAR(START_TIME, \'HH24:MI\') HOUR, "GW_NAME", "CPU_ID", "START_TIME", "INSERT_TIME", "DURATION", "CPU_USAGE", "MEMORY_USAGE", "AVG_CPU_USAGE", "PEAK_CPU_USAGE", "AVG_MEMORY_USAGE" FROM HUAWEI_PGW_SGW_EPC.SYS_RES_MEAS_CPU\@opt) WHERE START_TIME BETWEEN DATA AND DATA +0.99999'
        test_value = _check_type_starttime(ddl_test2)
        msg = 'Errore ad estrarre la stringa %s sulla funzione %s' % (check_starttime_2, '_check_type_starttime')
        self.assertEqual(test_value, check_starttime_2, msg)

    def test_read_inputfile(self):
        directory_export_ddl = 'D:\\rgarofal_DOCUMENT\\RTI_SPINDOX\\EXPORT\\TEST'
        filename = name_file_report_export % 'PROVA'
        file_dir = directory_export_ddl + "\\"
        with file(file_dir + filename, "r") as file_input:
            lines = file_input.readlines()
            for line in lines:
                print line

    def test_read_ddl_statement(self):
        print "READ DDL STATEMENT BEGIN"
        directory_export_ddl = 'D:\\rgarofal_DOCUMENT\\RTI_SPINDOX\\EXPORT\\TEST'
        filename = name_file_report_export % 'PROVA'
        file_dir = directory_export_ddl + "\\"
        with file(file_dir + filename, "r") as file_input:
            lines = file_input.readlines()
        result = list(itertools.takewhile(lambda x: end_patterns_str not in x,
                                          itertools.dropwhile(lambda x: start_patterns_str not in x, lines)))
        cc = len(result)
        print result
        while result:
            result = list(itertools.takewhile(lambda x: end_patterns_str not in x,
                                              itertools.dropwhile(lambda x: start_patterns_str not in x, lines[cc:])))
            cc += len(result)
            print result
        print "END DDL STATEMENT BEGIN"

    def test_check_starttime_in_all(self):
        res = extract_starttime(self.ddl_old_test)
        self.assertEqual(res, 'STARTTIME')

    def test_new_ddl(self):
        stats_sample_list = []
        ddl_result = ['CREATE VIEW LTE.V_ERI_GGSN_APN_FBC_CCAS_STATS\n',
                      'AS SELECT   "DATA", "HOUR",  "GGSN_NAME", "CC_AS_ID", "APN_NAME", "STARTTIME", "ZONE",\n',
                      '                "DURATION", "INSERT_TIME", "GGSNAPNFBCCCASNAME",\n',
                      '                "GGSNAPNFBCCCASSTARTREQ", "GGSNAPNFBCCCASSTARTREQFAIL",\n',
                      '                "GGSNAPNFBCCCASUPDATEREQ", "GGSNAPNFBCCCASUPDATEREQFAIL",\n',
                      '                "GGSNAPNFBCCCASSTOPREQ", "GGSNAPNFBCCCASSTOPREQFAIL",\n',
                      '                "GGSNAPNFBCCCASUSERSERVICEDEN", "GGSNAPNFBCCCASUSERUNKNOWN",\n',
                      '                "GGSNAPNSACCCCASAUTHREJECT", "GGSNAPNSACCCCASCCNOTAPPL"\n',
                      '      FROM  (SELECT TRUNC(STARTTIME) DATA, TO_CHAR(STARTTIME,\'HH24:MI\') HOUR, "GGSN_NAME", "CC_AS_ID", "APN_NAME", "STARTTIME", "ZONE",\n',
                      '                "DURATION", "INSERT_TIME", "GGSNAPNFBCCCASNAME",\n',
                      '                "GGSNAPNFBCCCASSTARTREQ", "GGSNAPNFBCCCASSTARTREQFAIL",\n',
                      '                "GGSNAPNFBCCCASUPDATEREQ", "GGSNAPNFBCCCASUPDATEREQFAIL",\n',
                      '                "GGSNAPNFBCCCASSTOPREQ", "GGSNAPNFBCCCASSTOPREQFAIL",\n',
                      '                "GGSNAPNFBCCCASUSERSERVICEDEN", "GGSNAPNFBCCCASUSERUNKNOWN",\n',
                      '                "GGSNAPNSACCCCASAUTHREJECT", "GGSNAPNSACCCCASCCNOTAPPL"\n',
                      ' FROM ERICSSON_GGSN.APN_FBC_CCAS_STATS@opt ) WHERE STARTTIME BETWEEN DATA AND DATA +0.99999;']

        name_view, new_ddl, old_view, stats_sample_list = _build_new_ddl(self.ddl_old_test, 'ERICSSON_GGSN', 'LTE',
                                                                         'opt',
                                                                         self.mapping_schema_test, self.mapping_schema_subcat_test)
        print name_view
        self.assertEqual(name_view, 'APN_FBC_CCAS_STATS')
        i = 0
        for line in ddl_result:
            line_out = new_ddl[i]
            self.assertEqual(line.strip().replace(" ", ""), line_out.strip().replace(" ", ""))
            i += 1

    def test_ddl_produced(self):
        list_of_schema_to_import = ['ERICSSON_MME_EPC']


#
# Per verificare quale tipo di colonna start time esiste
#
def extract_starttime(ddl_old):
    for str in ddl_old:
        res = _check_type_starttime(str)
        if res:
            return res
    return None


#
#
# private procedure to extract the position of the element with the FROM statements that
# could be change
#
def _extract_element_with_from(dll):
    i = 0
    for str in dll:
        if re.search('FROM', str.upper()):
            return i
        i += 1


#
# private procedure to manipulate the ddl to create the new view on the schema_import
# change the schema, analyze the starttime and add the two columns , change the db link
#
def _build_new_ddl(ddl_old, schema_export, schema_import, dblink, mapping_schema, mapping_schema_subcat):
    '''
       ddl_old :       (String) the old ddl statements
       schema_export : (String) This is the schema from which the view is extracted
       schema_import : (String) This is the destination schema
       dblink        : (String) this is the dblink to access the original table/view new name in RTI
       mapping_schema: (Dictionary) map between view and prefix to add to the new name
       mappng_schema_subcat: (Dictionary) map between view and suffix (subcategory) to add to the new name
    '''
    new_ddl = []
    # Generazione nome view
    import re
    import string

    suffix_subcat = mapping_schema_subcat[schema_export]
    last_clause_adding = ' WHERE %s BETWEEN DATA AND DATA +0.99999;\n'
    adding_columns = ' "DATA", "HOUR", '
    heading_from_select_adding = ' FROM (SELECT TRUNC(%s) DATA, TO_CHAR(%s,\'HH24:MI\') HOUR, '
    reg = r'(?P<schema>\w*)[.](?P<name_view>\s*\w*)'
    pattern = re.compile(reg)
    result = pattern.search(ddl_old[0])
    schema = result.group('schema')
    view_name = result.group('name_view')
    old_view = '%s.%s' % (schema, view_name)
    head_new_view = mapping_schema[schema]
    new_name_view = '%s_%s%s' % (head_new_view, view_name, suffix_subcat)
    # filtro del nome delle viste se necessario XXX
    new_name_view_result = {key: mapping[key] for key in mapping if key == new_name_view}
    if bool(new_name_view_result):
        new_name_view = mapping[new_name_view]
    new_view = '%s.%s' % (schema_import, new_name_view)
    new_ddl.append(string.replace(ddl_old[0], old_view, new_view))
    print "Elaborazione vista = %s" % old_view
    # cercare lo start time
    str_starttime = extract_starttime(ddl_old)
    if str_starttime:
        stats_sample = _build_stats_sample_statements(str_starttime, new_view)

        # sostituisce nel template la stringa
        heading_select_view_adding = 'AS SELECT %s' % adding_columns
        adding_from_select = heading_from_select_adding % (str_starttime, str_starttime)
        new_ddl.append(string.replace(ddl_old[1], 'AS SELECT ', heading_select_view_adding))
        ind_from = _extract_element_with_from(ddl_old)
        length = len(ddl_old)
        if length == ind_from:
            print "ERRORE nella parsificazione dello statement DDL. Non si trova lo statement FROM"
            return None, None
        # copia fino alla parte statement FROM esclusa
        for i in range(2, ind_from):
            new_ddl.append(ddl_old[i])

        initial_list_columns = ddl_old[1].replace('AS SELECT ', '')
        new_from_statement = adding_from_select + initial_list_columns
        new_ddl.append(new_from_statement)
        for i in range(2, ind_from):
            new_ddl.append(ddl_old[i])
        last_clause_adding = last_clause_adding % str_starttime
        end_ddl_clause = ' FROM %s@%s ) %s' % (old_view, dblink, last_clause_adding )
        new_ddl.append(end_ddl_clause)
    else:
        print "ERRORE nella parsificazione dello statement DDL. Non si trova lo start time"
        return None, None
    return view_name, new_ddl, old_view, stats_sample


#
# Crea gli statement per calcolare alcuni dati sulle tabelle origine
#
def _build_stats_statements(schema, old_schema_view):
    sql_statement = 'select \'%s\' as TABLE_NAME , AVG(count(*)) from %s group by TO_CHAR(insert_time, \'DD-MON-YY\');\n' % (
        old_schema_view, old_schema_view)
    return sql_statement


def _build_stats_sample_statements(startime, newschema_view):
    sql_statement = 'select \'%s\' AS TABLE_NAME, TO_CHAR(%s, \'DD-MON-YY HH24:MI\') %s from %s where rownum < 30;\n' % (
        newschema_view, startime, startime, newschema_view)
    return sql_statement;


#
# Dizionario: < VISTA > : < statement creazione vista >
#
def build_creating_statements(schema_export, schema_import, dblink, ddl_finals, stats_create):
    """


    :type dblink: string
    :param schema_export:
    :param schema_import: 
    :param dblink: 
    :param ddl_finals: 
    :return: 
    """
    stats_sample_list = []
    name_file_ddl = name_file_report_export % schema_export

    with file(directory_export_ddl + "\\" + name_file_ddl, "r") as input_file:
        lines = input_file.readlines()
    lines = list(itertools.dropwhile(lambda x: start_patterns_str not in x, lines))
    # OLD CODE
    # result = list(itertools.takewhile(lambda x: end_patterns_str not in x, itertools.dropwhile(lambda x: start_patterns_str not in x, lines)))
    # cc = len(result)
    #print result
    max_lenght = len(lines) - 1
    cc = 0
    #for ddl_old in result:
    #      result = list(itertools.takewhile(lambda x: end_patterns_str not in x, itertools.dropwhile(lambda x: start_patterns_str not in x, lines[cc:])))
    #      cc += len(result)
    #      if ddl_old:
    #          name_view, ddl_new = _build_new_ddl(ddl_old, schema_export, schema_import, dblink, mapping_schema)
    #          ddl_finals[name_view] = ddl_new
    while cc <= max_lenght:
        result = list(itertools.takewhile(lambda x: end_patterns_str not in x,
                                          itertools.dropwhile(lambda x: start_patterns_str not in x, lines[cc:])))
        #for views in result:
        #    print "Lettura viste %s"%views
        #print "Lunghezza lista di lettura %s"%len(result)
        cc += len(result) + 1
        #print "Next indice %s"% cc
        name_view, ddl_new, old_view, stats_sample_dml = _build_new_ddl(result, schema_export, schema_import, dblink,
                                                                        mapping_schema, mapping_schema_subcat)
        stats_sample_list.append(stats_sample_dml)
        stats_create[old_view] = _build_stats_statements(schema_export, old_view)
        ddl_finals[name_view] = ddl_new
    return len(ddl_finals.keys()), stats_sample_list

    #      if ddl_old:
    #          name_view, ddl_new = _build_new_ddl(ddl_old, schema_export, schema_import, dblink, mapping_schema)
    #          ddl_finals[name_view] = ddl_new


#
# Check
#
def _check_type_starttime(ddl_input):
    """

    :param ddl_input:
    :return: string
    """
    if re.search(check_starttime_1.upper(), ddl_input.upper()):
        return check_starttime_1
    elif re.search(check_starttime_2.upper(), ddl_input.upper()):
        return check_starttime_2
    else:
        return None


#
# Procedura di esempio per estrarre automaticamente lo statement di dll da usare
# problems su optima che non ha i grant necessari per estarre la struttura
# def create_view_statement(prefix_name_view, destination_schema, source_schema, db_conn, name_view_to_search):
# '''
#
# estrae la ddl della vista name_view_to_search dal database sorgente db_conn
#       vede se c'e' starttime  start_time
#       compone il nuovo statement di creazione usando destination_schema, il prefix_name_view
#    '''
#    ddl_start = extract_ddl_object('VIEW', name_view_to_search, source_schema, db_conn)


def list_of_views(owner, type_obj, connection, exclude_list):
    print 'Extract all %s from schema %s \n' % (type_obj, owner)

    sql_statement = extract_all_view_of_schema % (type_obj, owner)
    if exclude_list:
        sql_statement = sql_statement % exclude_list
    print sql_statement
    curs = execute_sql(sql_statement, connection)
    list_name = []
    for row in curs:
        name = ''.join(row)
        list_name.append(name)
    return list_name


def extract_ddl_object(obj_type, obj_name, schema_input, connection):
    """
    :rtype : list
    :param obj_type:
    :param obj_name:
    :param schema_input:
    :param connection:
    :return curs:
    """
    obj_type_s = "'%s'" % obj_type
    obj_name_s = "'%s'" % obj_name
    schema_s = "'%s'" % schema_input

    #sql_ddl = 'set heading off; set echo off; set pages 999; set long 90000; select dbms_metadata.get_ddl(%s,%s,%s) from dual' % (obj_type_s, obj_name_s, schema_s)
    #print sql_ddl
    sql_ddl = 'select dbms_metadata.get_ddl(%s,%s,%s) from dual' % (obj_type_s, obj_name_s, schema_s)
    print sql_ddl
    curs = execute_sql(sql_ddl, connection)
    return curs


def ora_connect(ip, sid, user, passwd):
    """
       This procedure connect to a Database Oracle
    :param ip:
    :param sid:
    :param user:
    :param passwd:
    :return:
    """
    print "Connecting to %s" % sid
    connection_string = '%s/%s@%s/%s' % (user, passwd, ip, sid)
    con = cx_Oracle.connect(connection_string)
    print con.version
    #con = cx_Oracle.connect('rti/wrti@10.178.7.187/RTI.vodafone.com')
    return con


def execute_sql(sql, connection):
    """
       This procedure executes a query select and return a cursor
    :param sql:
    :param connection:
    :return: cursor
    """
    cur = connection.cursor()
    cur.execute(sql)
    return cur


#
# Procedura per aggiornare automaticamente il catalogo
# Il file di partenza e quello generato da SQL developer nella creazione delle viste REPORT_IMPORT_RM132.log
#

#
#
#
def get_category_and_macro_cat (directory, report_created_view, map_category_and_new_schema, map_schema_to_macro_category ):

    lines = []
    import re
    category_result = ''
    macro_cat_result = ''
    reg = r'(?P<schema>\w*)[.](?P<name_view>\s*\w*)'
    pattern = re.compile(reg)

    with file(directory + "\\" + report_created_view, "r") as input_file:
        lines = input_file.readlines()
    for views in lines:
        result = pattern.search(views)
        schema = result.group('schema')
        view_name = result.group('name_view')
        if schema in map_category_and_new_schema and schema in map_schema_to_macro_category:
           category_result = map_category_and_new_schema[schema]
           macro_cat_result = map_schema_to_macro_category[schema]
           break
    return category_result, macro_cat_result

#
# Creazione dello script per creare sinonimi
#
def create_syn_to_rti(report_created_view, directory):
    sql_statement = 'CREATE OR REPLACE SYNONYM RTI.%s FOR %s.%s;\n'
    syn_script = 'CREATE_SYN_SCRIPTS.sql'
    lines = []
    import re

    reg = r'(?P<schema>\w*)[.](?P<name_view>\s*\w*)'
    pattern = re.compile(reg)

    with file(directory + "\\" + report_created_view, "r") as input_file:
        lines = input_file.readlines()
    with file(directory + "\\" + syn_script, "w") as output_file:
        lines_to_write = []
        for views in lines:
            result = pattern.search(views)
            schema = result.group('schema')
            view_name = result.group('name_view')
            sql_statement_write = sql_statement % (view_name, schema, view_name)
            lines_to_write.append(sql_statement_write)
        output_file.writelines(lines_to_write)


#
# Procedura per creare lo script di generazione viste
#
def create_script_for_views(dblink):
    for schema in list_of_schema_to_import.keys():
        # ATTENZIONE: Ho fatto l'assunzione che lo schema di destinazione coincida con la category
        category_extract = list_of_schema_to_import[schema]
        new_view = dict()
        stats_sql = dict()
        list_stats_samples = []

        try:
            # Old code num_total_view_for_schema, list_stats_samples = build_creating_statements(schema, 'LTE', 'opt', new_view, stats_sql)

            num_total_view_for_schema, list_stats_samples = build_creating_statements(schema, category_extract, dblink,
                                                                                      new_view, stats_sql)
            name_file = name_file_report_import % schema
            namefile_stats_script = namefile_stats_scripts_head % schema
            namefile_report_log = namefile_report_log_import % schema
            name_file_sample_scripts = 'GET_SAMPLES_VIEW_CAT_%s.sql' % schema
            final_dict = {key: new_view[key] for key in new_view if key not in filter_views}
            final_stats_sql = {key: stats_sql[key] for key in stats_sql if key not in filter_views}
            num_views_selected = len(final_dict.keys())

            with file(directory_export_ddl + "\\" + namefile_report_log, "w") as log_file:
                log_file.writelines("Report viste migrate dallo schema %s\n" % schema)
                log_file.writelines("Numero totale view estratte = %s - Numero totale view da implementare = %s \n" % (
                    num_total_view_for_schema, len(final_dict.keys())))
                for key in final_dict.keys():
                    report_line = '%s\n' % key
                    log_file.write(report_line)
                    # Creazione script di
            with file(directory_export_ddl + "\\" + name_file, "w") as output_file:
                for val in final_dict.itervalues():
                    output_file.writelines(val)
            with file(directory_export_ddl + "\\" + namefile_stats_script, "w") as out_stats_file:
                for val in final_stats_sql.itervalues():
                    out_stats_file.writelines(val)
            with file(directory_export_ddl + "\\" + name_file_sample_scripts, "w") as out_stats_file:
                out_stats_file.writelines(list_stats_samples)
        except IOError:
            pass

def create_script_for_cat_tab(directory, name_file_script, report_filename, map_category_and_new_schema):
    import re

    reg = r'(?P<schema>\w*)[.](?P<name_view>\s*\w*)'
    pattern = re.compile(reg)
    with file(directory + "\\" + report_filename, "r") as list_view_file:
        list_view_names = list_view_file.readlines()
    list_cat_tab = []
    for schema_and_view in list_view_names:
        result = pattern.search(schema_and_view)
        category = map_category_and_new_schema[result.group('schema')]
        view = result.group('name_view')
        sql_statement_insert = dml_cat_tab % (view, category)
        list_cat_tab.append(sql_statement_insert)
    with file(directory + "\\" + name_file_script, "w") as script_file:
        script_file.writelines(list_cat_tab)


def create_script_for_tab_query(directory, name_file_script, name_file_script_checks, name_file_script_update,
                                report_filename, map_category_and_new_schema, list_view_to_exclude,
                                list_columns_must_filled):
    import re

    # Associazione SOTTOCATEGORIA (foglio excel)  e prefisso view
    aggregate_mapping = dict()
    aggregate_mapping = dict(V_ERI_MME='ERICSSON MME', V_HUA_MME='HUAWEI MME', V_ERI_PGW='ERICSSON GTW',
                             V_ERI_GGSN='ERICSSON GTW', V_ERI_SGW='ERICSSON GTW', V_ERI_NOD='ERICSSON GTW',
                             V_CDR_CDR='CG HUAWEI', V_ALU='DRN')
    default_aggregate = 'HUAWEI GTW'
    reg = r'(?P<schema>\w*)[.](?P<name_view>\s*\w*)'
    pattern = re.compile(reg)
    with file(directory + "\\" + report_filename, "r") as list_view_file:
        list_view_names = list_view_file.readlines()
    list_table_to_elaborate = []
    list_cat_tab = []
    list_tab_fields = []
    list_tab_checks_fields = []

    list_tab_must_filled = []
    list_in_columns = ''
    for column_name in list_columns_must_filled:
        column_name = '\'%s\'' % (column_name)
        list_in_columns = ',' + column_name
    list_in_columns = list_in_columns[1:]
    for schema_and_view in list_view_names:
        result = pattern.search(schema_and_view)
        category = map_category_and_new_schema[result.group('schema')]
        view = result.group('name_view')
        name_query = view[2:]
        prefix = view[0:9]
        if prefix in aggregate_mapping:
            name_aggr = aggregate_mapping[prefix]
        else:
            for aggregate in aggregate_mapping:
                if aggregate in prefix:
                    name_aggr = aggregate_mapping[aggregate]
                    break
                else:
                    name_aggr = default_aggregate

        if view not in list_view_to_exclude:
            sql_statement_insert = dml_query_tab % (view, category, name_query, name_aggr)
            sql_statement_insert_fields = dml_query_fields % (view, category, name_query)
            sql_statement_checks_fields = dml_query_checks_columns % (name_query)
            sql_update_must_filled = dml_update_usage_columns % (list_in_columns, view)
            list_cat_tab.append(sql_statement_insert)
            list_tab_fields.append(sql_statement_insert_fields)
            list_tab_checks_fields.append(sql_statement_checks_fields)
            list_tab_must_filled.append(sql_update_must_filled)
            list_table_to_elaborate.append(view)
    with file(directory + "\\" + name_file_script, "w") as script_file:
        script_file.writelines(list_cat_tab)
        script_file.writelines(list_tab_fields)
    with file(directory + "\\" + name_file_script_checks, "w") as script_file:
        script_file.writelines(list_tab_checks_fields)
    with file(directory + "\\" + name_file_script_update, "w") as script_file:
        script_file.writelines(list_tab_must_filled)
    return list_table_to_elaborate


def create_script_check_existence(directory, name_file_script, list_view_to_elaborate):
    dml_query_checks = 'SELECT COUNT(*) FROM TAB_QUERY WHERE NOME_TAB = \'%s\';\n'
    list_select_tables = []
    for table_name in list_view_to_elaborate:
        query_checks = dml_query_checks % table_name
        list_select_tables.append(query_checks)
    with file(directory + "\\" + name_file_script, "w") as script_file:
        script_file.writelines(list_select_tables)


def create_script_for_users_query(directory, name_file_script, category, codice_mcategory, list_view_to_elaborate,
                                  list_grants):
    list_insert_grants = []
    for name_table in list_view_to_elaborate:
        query_name = name_table[2:]
        for user_name in list_grants:
            sql_statement = dml_insert_grants % (user_name, query_name, name_table, category, codice_mcategory)
            list_insert_grants.append(sql_statement)
    with file(directory + "\\" + name_file_script, "w") as script_file:
        script_file.writelines(list_insert_grants)


# Schema file di configurazione per estrarre le viste da configurare
# Database IP  user password  SID  Nome Schema dblink
# 10.x.y.z     pippo          LAB  SCHEMA_1    OPT
#


if __name__ == '__main__':
    import os
    import sys

    print "Starting ...."
    #1 Create the script to cfreate the view with new 2 columns
    #  create the synonym on RTI

    #Old Code create_script_for_views('LTE', 'opt')

    create_script_for_views('opt')
    #report_created_view = 'REPORT_IMPORT_RM132.log'
    report_files = directory_export_ddl + '\\' + report_created_view
    if not os.path.exists(report_files):
        print "Restart the program after produced the report of the view created REPORT_IMPORT_RM132"
        sys.exit()
    category_name ,category_codice = get_category_and_macro_cat (directory_export_ddl, report_created_view, map_category_and_new_schema, map_schema_to_macro_category)
    create_syn_to_rti(report_created_view, directory_export_ddl)

    # 2 Create the script to add to CAT_TAB
    # 3 Create the script to make the checks
    # 4 Create the script to update the attribute to indicate that the column value is required in the analysis
    #   you can add the column required in the list list_columns_to_update

    # old code category_name = 'LTE'
    name_file_script = 'INSERT_CAT_TAB_VIEWS_CATEG_%s.sql' % (category_name)
    create_script_for_cat_tab(directory_export_ddl, name_file_script, report_created_view, map_category_and_new_schema)
    # add a filter if any view is already present
    list_view_to_exclude = ['V_HUA_MME_S1_MOB_MGMT', 'V_HUA_MME_S1_SESS_MGMT', 'V_HUA_MME_S1_INTERFACE',
                            'V_HUA_MME_SYSTEM_RESOURCES', 'V_HUA_MME_IU_MEAS', 'V_HUA_MME_IU_SM_MEAS_PLMN',
                            'V_HUA_MME_IU_SM_MEAS_UE', 'V_HUA_MME_IU_MM_MEAS_PLMN', 'V_HUA_MME_IU_MM_MEAS_UE']
    name_file_script = 'INSERT_TAB_QUERY_CATEG_%s.sql' % (category_name)
    name_file_script_checks = 'CHECKS_TAB_QUERY_CATEG_%s.sql' % (category_name)
    name_file_script_update = 'UPDATE_MUSTFILLED_TAB_QUERY_CATEG_%s.sql' % category_name
    list_columns_to_update = ['DATA', ]
    list_table_to_elaborate = create_script_for_tab_query(directory_export_ddl, name_file_script,
                                                          name_file_script_checks,
                                                          name_file_script_update, report_created_view,
                                                          map_category_and_new_schema, list_view_to_exclude,
                                                          list_columns_to_update)
    # 5 Add the user to grants to the analysis
    #   list_grants is the list to add the user id to grant
    #   it is possible to pass a list of view to update using the list_view_to_elaborate
    #   To apply the setting on all view delete explicit setting of the list_view_to_elaborate

    list_grants = ['ABIAVA', 'GCERUTT', 'GLAMEDI', 'PPERICC', 'SMANTOA', 'VMARI04', 'ZZAGH15', 'ZZGGU53', 'ZZMC696']

    #list_table_to_elaborate = [
    #    'V_ERI_MME_LLC_MEAS',
    #    'V_ERI_MME_OVERLOAD_MEAS',
    #    'V_ERI_MME_QOS_MEAS',
    #    'V_ERI_MME_RANAP_RNC_MEAS',
    #    'V_ERI_MME_SGS_MEAS',
    #    'V_ERI_MME_SNDCP_MEAS',
    #    'V_ERI_MME_SUBS_MEAS',
    #    'V_ERI_MME_SYS_MEAS_NE',
    #    'V_ERI_PGW_GGSN_APN_FBC',
    #    'V_ERI_PGW_GGSN_APN_FBC_CCAS',
    #    'V_ERI_PGW_GGSN_APN_FBC_RULESET',
    #    'V_ERI_PGW_GGSN_APN_FBC_SERVCLS',
    #    'V_ERI_PGW_GGSN_APN_FBC_SERVID',
    #    'V_ERI_PGW_GGSN_APN_SACC_RG',
    #    'V_ERI_PGW_GGSN_APN_SACC3_SERVD',
    #    'V_ERI_PGW_GGSN_FBC_DIAM',
    #    'V_ERI_PGW_GGSN_GTP_ACC_CG_SERV',
    #    'V_ERI_PGW_GGSN_L2TP_STATS',
    #    'V_ERI_PGW_GGSN_RADIUS_ACC',
    #    'V_ERI_PGW_GGSN_RADIUS_AUTH',
    #    'V_ERI_PGW_NODE_STATS',
    #    'V_ERI_PGW_SGW_PATH_MGMT',
    #    'V_ERI_PGW_SGW_TUNNEL_MGMT']

    name_file_script = 'CHECK_LIST_TABLES_CATEG_%s.sql' % (category_name)
    create_script_check_existence(directory_export_ddl, name_file_script, list_table_to_elaborate)
    name_file_script = 'INSERT_QUERY_GRANTS_CATEG_%s.sql' % (category_name)
    create_script_for_users_query(directory_export_ddl, name_file_script, category_name, category_codice,
                                  list_table_to_elaborate, list_grants)








# (RN75) conn = ora_connect('10.192.26.184','RTI.vodafone.com', 'rti', 'wrti')
# (OPTIMA) conn = ora_connect('10.23.41.12','repdb', 'nwis', 'woptnis02')
#conn = ora_connect('10.178.7.187','RTI.vodafone.com', 'rti', 'wrti')
#list_view = list_of_views('\'LTE\'', type_obj_view, conn)
#print list_view
#
#select OBJECT_NAME from all_objects where OBJECT_TYPE = 'VIEW' and owner = 'LTE'
# [('V_HUA_SYS_RES_CPU_MEAS',), ('V_HUA_PGW_SESSION_MEAS',), ('V_HUA_PGW_FW_MEAS',), ('V_HUA_PGW_FW_APN_MEAS',), ('V_HUA_MME_GB_SM_MEAS',), ('V_HUA_MME_GB_MM_MEAS',), ('V_HUA_MME_GB_MEAS',), ('V_ERI_SGW_SESSIONS',), ('V_ERI_PGW_APN_DATA_PLANE',), ('V_ERI_PGW_APN_BEARERS',), ('V_ERI_MME_SM_MEAS',), ('V_ERI_MME_MM_MEAS',), ('V_ERI_MME_GTP_MEAS',), ('V_ERI_MME_BSSGP_MEAS',), ('V_ERI_GGSN_U_PIC',)]
#
