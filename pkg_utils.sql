/*
   Created by Ahmed Fikri (2020)
   useful functions
*/
CREATE OR REPLACE PACKAGE pkg_utils
AS
TYPE t_line IS RECORD (line VARCHAR2 (4000));

TYPE t_lines IS TABLE OF t_line;
-- pipelined function to read a file line by line as table
-- SELECT * FROM TABLE(pkg_utils.fu_read_file_as_tbl('dummy.txt','TMP')) 
FUNCTION fu_read_file_as_tbl(p_filename VARCHAR2, p_dir VARCHAR2)
RETURN t_lines
PIPELINED;
END pkg_utils;
/
CREATE OR REPLACE PACKAGE BODY pkg_utils
AS

FUNCTION fu_read_file_as_tbl(p_filename VARCHAR2, p_dir VARCHAR2)
RETURN t_lines
PIPELINED
IS
  v_line t_line;
  v_file UTL_FILE.FILE_TYPE;
BEGIN
  v_file := UTL_FILE.FOPEN(upper(p_dir),p_filename,'R');
  dbms_output.put_line('begin');
  IF UTL_FILE.IS_OPEN(v_file) THEN
    LOOP
      BEGIN
        UTL_FILE.GET_LINE(v_file,v_line.line);
        PIPE ROW (v_line);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        EXIT;   
    END;
  END LOOP;
  dbms_output.put_line('end');
  utl_file.fclose(v_file);
  END IF;
  EXCEPTION
    --don't use others: Bug 13088409 was fixed in 11.2.0.2 ( Exceptions raised in a WHEN OTHERS clause of a pipelined function were ignored)
    --instead use NO_DATA_NEEDED 
    --ORA-06548: no more rows needed
    WHEN NO_DATA_NEEDED THEN
      dbms_output.put_line('exception in ' || SQLERRM);  
      IF UTL_FILE.IS_OPEN(v_file) THEN
        utl_file.fclose(v_file);
      END IF;
      dbms_output.put_line('exception out');
END;

END pkg_utils;
/
