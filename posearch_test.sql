set SERVEROUTPUT ON;
 declare 
 
 I_PO_NBR NUMBER;
  I_ORDER_NO NUMBER;
  I_OPTION_ID VARCHAR2(25);
  I_PO_STATUS VARCHAR2(1);
  I_SUPPLIER NUMBER;
  I_PO_SOURCE VARCHAR2(200);
  I_HANDOVER_DATE VARCHAR2(200);
  I_HANDOVER_DATE_END VARCHAR2(200);
  I_DEPT_LIST VARCHAR2(200);
  I_CLASS_LIST VARCHAR2(200);
  I_SUBCLASS_LIST VARCHAR2(200);
  I_BUSINESS_MODEL VARCHAR2(250);
  I_BUSINESS_MODEL_LIST VARCHAR2(200);
  I_BUYING_GROUP_KEY VARCHAR2(200);
  I_BUYING_GROUP_LIST VARCHAR2(200);
  I_BUYING_SUBGROUP_LIST VARCHAR2(200);
  I_BUYING_SET_LIST VARCHAR2(200);
  I_EXECUTE VARCHAR2(200);
  I_FACTORY VARCHAR2(10);
  I_UPLOAD_ID VARCHAR2(30);
  I_UPLOAD_CREATE_DATE DATE;
  v_Return RMS16MA.POSEARCHLHBTBL;
BEGIN
  I_PO_NBR := NULL;
  I_ORDER_NO := NULL;
  I_OPTION_ID := NULL;
  I_PO_STATUS := NULL;
  I_SUPPLIER := NULL;
  I_PO_SOURCE := NULL;
  I_HANDOVER_DATE := NULL;
  I_HANDOVER_DATE_END := NULL;
  I_DEPT_LIST := NULL;
  I_CLASS_LIST := NULL;
  I_SUBCLASS_LIST := NULL;
  I_BUSINESS_MODEL := NULL;
  I_BUSINESS_MODEL_LIST := NULL;
  I_BUYING_GROUP_KEY := NULL;
  I_BUYING_GROUP_LIST := NULL;
  I_BUYING_SUBGROUP_LIST := NULL;
  I_BUYING_SET_LIST := NULL;
  I_EXECUTE := 'Y';
  I_FACTORY := NULL;
  I_UPLOAD_ID := NULL;
  I_UPLOAD_CREATE_DATE := NULL;

    SELECT --*  
  POSEARCHLHBOBJ(ORD_LEVEL,MASTER_ORDER_NO,ORDER_NO,OPTION_ID,ITEM_DESC,ITEM_STATUS_DESC,STATUS,STATUS_DESC,SUPPLIER,SUPPLIER_REFERENCE,
              SUPP_DESC,EXPECTED_DELIVERY_DATE,CREATED_BY,CREATE_DATETIME,FC,PO_TYPE,PO_SOURCE,HANDOVER_DATE,HANDOVER_END_DATE,
              PRODUCT_GROUP,PRODUCT_GROUP_DESC,CATEGORY,CATEGORY_DESC,SUB_CATEGORY,SUB_CATEGORY_DESC,CLASS_KEY,
              SUBCLASS_KEY,BUSINESS_MODEL,BUSINESS_MODEL_NAME,BUYING_GROUP,BUYING_GROUP_NAME,BUYING_GROUP_KEY,
              BUYING_SUBGROUP,BUYING_SUBGROUP_NAME,BUYING_SUBGROUP_KEY,BUYING_SET,BUYING_SET_NAME,BUYING_SET_KEY,
              FACTORY,UNITS) 
           bulk collect into v_Return
  FROM TABLE(MA_ORDER_UTILS_SQL_LHB.PO_SEARCH(
    I_PO_NBR,
    I_ORDER_NO,
    I_OPTION_ID,
    I_PO_STATUS,
    I_SUPPLIER,
    I_PO_SOURCE,
    I_HANDOVER_DATE,
    I_HANDOVER_DATE_END,
    I_DEPT_LIST,
    I_CLASS_LIST,
    I_SUBCLASS_LIST,
    I_BUSINESS_MODEL,
    I_BUSINESS_MODEL_LIST,
    I_BUYING_GROUP_KEY,
    I_BUYING_GROUP_LIST,
    I_BUYING_SUBGROUP_LIST,
    I_BUYING_SET_LIST,
    I_EXECUTE,
    I_FACTORY,
    I_UPLOAD_ID,
    I_UPLOAD_CREATE_DATE
  ));
 /* 
DECLARE
  I_PO_NBR number := 186801;
  I_ORDER_NO NUMBER := 186802;
  I_OPTION_ID VARCHAR2(25);
  I_PO_STATUS VARCHAR2(1);
  I_SUPPLIER NUMBER;
  I_EXP_DELIVERY_DATE VARCHAR2(200);
  I_CREATED_BY VARCHAR2(30);
  I_FC NUMBER;
  I_PO_TYPE VARCHAR2(4);
  I_PO_SOURCE VARCHAR2(200);
  I_HANDOVER_DATE VARCHAR2(200);
  I_DEPT_LIST VARCHAR2(200);
  I_CLASS_LIST VARCHAR2(200);
  I_SUBCLASS_LIST VARCHAR2(200);
  I_BUSINESS_MODEL VARCHAR2(250);
  I_BUSINESS_MODEL_LIST VARCHAR2(200);
  I_BUYING_GROUP_KEY VARCHAR2(200);
  I_BUYING_GROUP_LIST VARCHAR2(200);
  I_BUYING_SUBGROUP_LIST VARCHAR2(200);
  I_BUYING_SET_LIST VARCHAR2(200);
  I_PROCESS_ID VARCHAR2(200);
  I_PROCESS_DATE DATE;
  I_EXECUTE VARCHAR2(200);
  I_FACTORY VARCHAR2(10);
  v_Return RMS16MA.POSEARCHTBL;
BEGIN
  I_PO_NBR := 186801;
  I_ORDER_NO := 186802;
  I_OPTION_ID := NULL;
  I_PO_STATUS := NULL;
  I_SUPPLIER := NULL;
  I_EXP_DELIVERY_DATE := NULL;
  I_CREATED_BY := NULL;
  I_FC := NULL;
  I_PO_TYPE := NULL;
  I_PO_SOURCE := NULL;
  I_HANDOVER_DATE := NULL;
  I_DEPT_LIST := NULL;
  I_CLASS_LIST := NULL;
  I_SUBCLASS_LIST := NULL;
  I_BUSINESS_MODEL := NULL;
  I_BUSINESS_MODEL_LIST := NULL;
  I_BUYING_GROUP_KEY := NULL;
  I_BUYING_GROUP_LIST := NULL;
  I_BUYING_SUBGROUP_LIST := NULL;
  I_BUYING_SET_LIST := NULL;
  I_PROCESS_ID := NULL;
  I_PROCESS_DATE := NULL;
  I_EXECUTE := 'Y';
  I_FACTORY := NULL;
--select * from table(ma_order_utils_sql_lhb.PO_SEARCH(I_execute => 'Y'));
    SELECT --*  
  POSEARCHOBJ(ORD_LEVEL,MASTER_ORDER_NO,ORDER_NO,OPTION_ID,ITEM_DESC,ITEM_STATUS_DESC,STATUS,STATUS_DESC,SUPPLIER,SUPP_DESC,EXPECTED_DELIVERY_DATE,CREATED_BY,FC,PO_TYPE,PO_SOURCE,HANDOVER_DATE,HANDOVER_END_DATE,PRODUCT_GROUP,PRODUCT_GROUP_DESC,CATEGORY,CATEGORY_DESC,SUB_CATEGORY,SUB_CATEGORY_DESC,CLASS_KEY,SUBCLASS_KEY,BUSINESS_MODEL,BUSINESS_MODEL_NAME,BUYING_GROUP,BUYING_GROUP_NAME,BUYING_GROUP_KEY,BUYING_SUBGROUP,BUYING_SUBGROUP_NAME,BUYING_SUBGROUP_KEY,BUYING_SET,BUYING_SET_NAME,BUYING_SET_KEY,FACTORY,UNITS) 
           bulk collect into v_Return
  FROM TABLE(MA_ORDER_UTILS_SQL_LHB.PO_SEARCH(
    I_PO_NBR,
    I_ORDER_NO,
    I_OPTION_ID,
    I_PO_STATUS,
    I_SUPPLIER,
    I_EXP_DELIVERY_DATE,
    I_CREATED_BY,
    I_FC,
    I_PO_TYPE,
    I_PO_SOURCE,
    I_HANDOVER_DATE,
    I_DEPT_LIST,
    I_CLASS_LIST,
    I_SUBCLASS_LIST,
    I_BUSINESS_MODEL,
    I_BUSINESS_MODEL_LIST,
    I_BUYING_GROUP_KEY,
    I_BUYING_GROUP_LIST,
    I_BUYING_SUBGROUP_LIST,
    I_BUYING_SET_LIST,
    I_PROCESS_ID,
    I_PROCESS_DATE,
    I_EXECUTE,
    I_FACTORY
  ));
  */
  /* Legacy output: */
  
  DBMS_OUTPUT.put_line (v_Return.COUNT);
  FOR l_row IN 1 .. v_Return.COUNT
     LOOP
        DBMS_OUTPUT.put_line (v_Return (l_row).item_desc);
        DBMS_OUTPUT.put_line (v_Return (l_row).master_order_no);
        DBMS_OUTPUT.put_line (v_Return (l_row).option_id);
        DBMS_OUTPUT.put_line (v_Return (l_row).EXPECTED_DELIVERY_DATE);
        DBMS_OUTPUT.put_line (v_Return (l_row).BUYING_SET);
        DBMS_OUTPUT.put_line (v_Return (l_row).UNITS);
        DBMS_OUTPUT.put_line (v_Return (l_row).BUYING_SUBGROUP_KEY);
        DBMS_OUTPUT.put_line (v_Return (l_row).BUYING_SET_NAME);
   --     DBMS_OUTPUT.put_line (v_Return (l_row).buyrarchy);
   --     DBMS_OUTPUT.put_line (v_Return (l_row).product_hierarchy);
   --     DBMS_OUTPUT.put_line (v_Return (l_row).asos_colour);	
   --    DBMS_OUTPUT.put_line (v_Return (l_row).unit_cost);
   --     DBMS_OUTPUT.put_line (v_Return (l_row).qty_ordered);
   --     DBMS_OUTPUT.put_line (v_Return (l_row).seq_no);		
        DBMS_OUTPUT.put_line (v_Return (l_row).factory);
  --      DBMS_OUTPUT.put_line (v_Return (l_row).manu_country_id);
  --      DBMS_OUTPUT.put_line (v_Return (l_row).size_group);	
  --      DBMS_OUTPUT.put_line (v_Return (l_row).supplier_reference);	
  --      DBMS_OUTPUT.put_line (v_Return (l_row).supplier_colour);
  --      DBMS_OUTPUT.put_line (v_Return (l_row).packing_method);
  --      DBMS_OUTPUT.put_line (v_Return (l_row).factory_risk_rating);		
  --      DBMS_OUTPUT.put_line (v_Return (l_row).create_id);
  --      DBMS_OUTPUT.put_line (v_Return (l_row).create_datetime);
  --      DBMS_OUTPUT.put_line (v_Return (l_row).last_update_id);	
  END LOOP;
 
/**/ 
  --:v_Return := v_Return;
--rollback; 
END;


