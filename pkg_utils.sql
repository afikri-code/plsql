/*
   Created by Ahmed Fikri (2020)
   useful functions
*/
CREATE OR REPLACE PACKAGE PKG_UTILS AS
  TYPE T_LINE IS RECORD(
    LINE VARCHAR2(4000));

  TYPE T_LINES IS TABLE OF T_LINE;
  -- pipelined function to read a file line by line as table
  -- SELECT * FROM TABLE(pkg_utils.fu_read_file_as_tbl('dummy.txt','TMP')) 
  FUNCTION FU_READ_FILE_AS_TBL(P_FILENAME VARCHAR2, P_DIR VARCHAR2)
    RETURN T_LINES
    PIPELINED;
  -- delete data from a table using partition exchange (simple example)  
  PROCEDURE pc_del_data_using_prtexchg(p_trgt_tbl VARCHAR2, p_where_cond VARCHAR2, p_tblspace VARCHAR2, p_dop VARCHAR2 DEFAULT '4');
  -- example to read a sql behind a ref cursor
  PROCEDURE pc_exp_read_sql_refcurs;
  --example to write the result of a query to a csv file
  PROCEDURE write_to_file(dirnam varchar2, filnam varchar2, p_sql VARCHAR2);
END PKG_UTILS;
/
CREATE OR REPLACE PACKAGE BODY PKG_UTILS AS

  FUNCTION FU_READ_FILE_AS_TBL(P_FILENAME VARCHAR2, P_DIR VARCHAR2)
    RETURN T_LINES
    PIPELINED IS
    V_LINE T_LINE;
    V_FILE UTL_FILE.FILE_TYPE;
  BEGIN
    V_FILE := UTL_FILE.FOPEN(UPPER(P_DIR), P_FILENAME, 'R');
    DBMS_OUTPUT.PUT_LINE('begin');
    IF UTL_FILE.IS_OPEN(V_FILE) THEN
      LOOP
        BEGIN
          UTL_FILE.GET_LINE(V_FILE, V_LINE.LINE);
          PIPE ROW(V_LINE);
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            EXIT;
        END;
      END LOOP;
      DBMS_OUTPUT.PUT_LINE('end');
      UTL_FILE.FCLOSE(V_FILE);
    END IF;
  EXCEPTION
    --don't use others: Bug 13088409 was fixed in 11.2.0.2 ( Exceptions raised in a WHEN OTHERS clause of a pipelined function were ignored)
    --instead use NO_DATA_NEEDED 
    --ORA-06548: no more rows needed
    WHEN NO_DATA_NEEDED THEN
      DBMS_OUTPUT.PUT_LINE('exception in ' || SQLERRM);
      IF UTL_FILE.IS_OPEN(V_FILE) THEN
        UTL_FILE.FCLOSE(V_FILE);
      END IF;
      DBMS_OUTPUT.PUT_LINE('exception out');
  END;
  
  --========================================================================================================
  -- Example for deleting data from a table using partition exchange
  --======================================================================================================== 
  PROCEDURE pc_del_data_using_prtexchg(p_trgt_tbl VARCHAR2, p_where_cond VARCHAR2, p_tblspace VARCHAR2, p_dop VARCHAR2 DEFAULT '4') IS  
  v_sql VARCHAR2(32767);
  v_tmp_tbl VARCHAR2(30) := 'tmp_tbl_to_purge_old_data';
  v_cols sys.odcivarchar2list;
  FUNCTION get_cols RETURN VARCHAR2 IS
    ret VARCHAR2(4000);
  BEGIN
    FOR i IN 1..v_cols.count LOOP ret := ret || v_cols(i) || ','; END LOOP;
    RETURN RTRIM(ret,',');
  END get_cols;
  PROCEDURE LOG(msg VARCHAR2) IS BEGIN dbms_output.put_line(msg); END;
BEGIN
  
  SELECT column_name BULK COLLECT INTO v_cols FROM user_tab_cols WHERE table_name = upper(p_trgt_tbl) ORDER BY column_id;
  IF v_cols.count = 0 THEN
    LOG('table ' || p_trgt_tbl || ' doen''t exist ');
    RETURN;
  END IF;  
  
  BEGIN
    EXECUTE IMMEDIATE 'drop table ' || v_tmp_tbl || ' purge';
    LOG('table ' || v_tmp_tbl  || ' dropped');
  EXCEPTION
    WHEN OTHERS THEN
      LOG('table ' || v_tmp_tbl || ' doesn''t exist, we continue');
   END ;
   
   v_sql := 'CREATE TABLE ' || v_tmp_tbl || ' ('||get_cols||') 
             partition by range ('||v_cols(1)||') (partition p0  values less than (maxvalue) tablespace '||p_tblspace||')
             as ( select /*+ parallel(t '||p_dop||') */ * from '||p_trgt_tbl||' t '||p_where_cond||')';
   LOG(v_sql);
   EXECUTE IMMEDIATE v_sql;
   LOG(SQL%ROWCOUNT || ' rows inseted into ' || v_tmp_tbl);
   
   v_sql := 'LOCK TABLE ' || v_tmp_tbl || ' PARTITION (p0) IN SHARE MODE';
   log(v_sql);
   EXECUTE IMMEDIATE v_sql;
   COMMIT;
   
   v_sql := 'ALTER TABLE ' || v_tmp_tbl || ' EXCHANGE PARTITION p0 WITH TABLE ' || p_trgt_tbl ;
         
   log('do delete');
   EXECUTE IMMEDIATE v_sql;
   EXECUTE IMMEDIATE 'drop table ' || v_tmp_tbl || ' purge';
   LOG('delete done');
   
EXCEPTION 
  WHEN OTHERS THEN
    LOG(SQLERRM);
    RAISE;
END pc_del_data_using_prtexchg;

  --========================================================================================================
  -- Example to read the sql behind a ref cursor
  -- remember grant select any dictionary TO user: in order to be able to use the v$ view in the procedure
  --!! select catalog role is a role, roles are not enabled during the compilation of plsql/views and are 
  -- not ever enabled during the execution of definers rights procedures
  -- so, select catalog role would let you write queries in sqlplus for example, but not create a procedure (as select any dictionary, the system privilege, does) 
  --========================================================================================================
PROCEDURE pc_exp_read_sql_refcurs IS
  v_refcurs SYS_REFCURSOR;
  v_text dual.dummy%TYPE;
BEGIN
  -- create dummy ref cursor
  OPEN v_refcurs FOR 'select dummy from dual where dummy = :1 and dummy = :2 and rownum < 11' USING 'X','Y';
  LOOP
    FETCH v_refcurs INTO v_text;
    EXIT WHEN v_refcurs%NOTFOUND;
  END LOOP;
  
  -- before closing the cursor, 
    FOR rec IN (SELECT s.sql_fulltext,oc.child_address FROM v$sql s JOIN  v$open_cursor oc ON s.sql_id = oc.sql_id 
      WHERE oc.SID = sys_context('userenv','sid') AND lower(s.sql_text) NOT LIKE '%v$%') LOOP
      dbms_output.put_line(rec.sql_fulltext);
      FOR r IN (SELECT * FROM v$SQL_BIND_CAPTURE WHERE child_address =  rec.child_address) LOOP
        dbms_output.put_line(r.name || ' = '  || r.value_string);
      END LOOP;
      dbms_output.put_line(RPAD('=',50,'='));
    END LOOP; 
  CLOSE v_refcurs;
END;
--========================================================================================================
-- write dynamically the result of query to csv file
-- call example: write_to_file('TMP_DIR','test.csv','SELECT * FROM TBL_B_0 WHERE ROWNUM < 10');
--========================================================================================================
PROCEDURE write_to_file(dirnam varchar2, filnam varchar2, p_sql VARCHAR2)
    IS
    v_columns dbms_sql.desc_tab;
    v_cursor_id integer;
    v_col_cnt integer;
    v_line VARCHAR2(32767) ;
    v_sql VARCHAR2(32767);
    v_delimiter CHAR(1) := ';';
    v_separator CHAR(1) := '"';
    lf CONSTANT VARCHAR2(1) := chr(10);
    export_charset VARCHAR2(30) := 'AL32UTF8';
    char_set VARCHAR2(30) := 'AL32UTF8';
BEGIN
  v_cursor_id := dbms_sql.open_cursor;
  dbms_sql.parse(v_cursor_id, p_sql, dbms_sql.native);
  dbms_sql.describe_columns(v_cursor_id, v_col_cnt, v_columns);
  dbms_sql.close_cursor(v_cursor_id);
  for i in 1 .. v_columns.count LOOP
    v_line := v_line||'||'''||v_separator||''||v_delimiter||''||v_separator||'''||curtype.'||v_columns(i).col_name;
  end loop;
  v_line := ''''||v_separator||'''||'||LTRIM(v_line,'||'''||v_separator||''||v_delimiter||''||v_separator||'''||')||'||'''||v_separator||'''';
  v_sql := 'declare'||CHR(10)||
           '  CURSOR mycur IS '||p_sql||';'||lf||
           '  curtype mycur%ROWTYPE;'||lf||
           '  fh utl_file.file_type;'||lf||
           'begin'||lf||
           '  fh := utl_file.fopen(''' || dirnam || ''', ''' || filnam || ''', ''W'', 32767);'||lf||
           '  OPEN mycur;'||lf||
           '  LOOP'||lf||
           '  FETCH mycur INTO curtype;'||lf||
           '  EXIT WHEN mycur%NOTFOUND;'||lf||
           '  utl_file.put_line(fh, convert('||v_line||', ''' || export_charset || ''', ''' || char_set || '''), TRUE);'||lf||
           '  END LOOP;'||lf||
           '  CLOSE mycur;'||lf||
           '  utl_file.fclose(fh);'||lf||
           '  dbms_output.put_line(''done'');'||lf||
           'exception'||lf||
           '  when others then'||lf||
           '    dbms_output.put_line(sqlerrm);'||lf||
           '    begin'||lf||
           '      utl_file.fclose(fh);'||lf||
           '      exception'||lf||
           '  when others then'||lf||
           '    dbms_output.put_line(sqlerrm);'||lf||
           '  end;'||lf||
           'end;';
   -- dbms_output.put_line(v_sql);
   EXECUTE IMMEDIATE v_sql;
END write_to_file;

END PKG_UTILS;
/
