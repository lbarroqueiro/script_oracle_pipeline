create or replace PACKAGE MA_ORDER_UTILS_SQL_LHB IS


--------------------------------------------------------------------------------
/*************************************************************************** **/
/* CREATE DATE - 25/10/2017                                                   */
/* CREATE USER - LUCIANO BARROQUEIRO                                          */
/* PROJECT     -                                                              */
/* DESCRIPTION - Package with global variables for ASOS MicroApps             */
/******************************************************************************/
--------------------------------------------------------------------------------

MICROAPP_ID     CONSTANT  VARCHAR2(10) := GLOBAL_VARS_SQL.G_ma_orders;


--------------------------------------------------------------------------------
FUNCTION GET_ORDER_SEQ
RETURN NUMBER;
--------------------------------------------------------------------------------
FUNCTION GET_ITEM_DIST_ID_SEQ
RETURN NUMBER;
--------------------------------------------------------------------------------
FUNCTION ORDER_LIST (I_master_order_no IN  ORDHEAD.MASTER_PO_NO%TYPE DEFAULT NULL)
RETURN MA_ORDER_LIST_TBL PIPELINED;
--------------------------------------------------------------------------------
FUNCTION GET_ORDHEAD_CFA_RMS(I_master_order_no  IN   MA_STG_ORDER_DROPS_DETAIL.MASTER_ORDER_NO%TYPE,
                             I_order_no         IN   MA_STG_ORDER_DROPS_DETAIL.ORDER_NO%TYPE,
                             I_cfa_type         IN   MA_CFA_CONF.CFA_TYPE%TYPE)
RETURN VARCHAR2;
--------------------------------------------------------------------------------
FUNCTION GET_ORDSKU_CFA_RMS(I_master_order_no  IN   MA_STG_ORDER_DROPS_DETAIL.MASTER_ORDER_NO%TYPE,
                            I_order_no         IN   MA_STG_ORDER_DROPS_DETAIL.ORDER_NO%TYPE,
                            I_item             IN   MA_STG_SIZING_SKU.SKU%TYPE,
                            I_cfa_type         IN   MA_CFA_CONF.CFA_TYPE%TYPE)
RETURN VARCHAR2;
--------------------------------------------------------------------------------
FUNCTION GET_PARTNER_CFA_RMS(I_partner_id       IN   PARTNER_CFA_EXT.PARTNER_ID%TYPE,
                             I_cfa_type         IN   MA_CFA_CONF.CFA_TYPE%TYPE)
RETURN VARCHAR2;
--------------------------------------------------------------------------------
FUNCTION CHECK_ORDER_LOCKS(O_error_message   OUT VARCHAR2,
                           I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN VARCHAR2;
--------------------------------------------------------------------------------
FUNCTION PO_SEARCH(I_po_nbr               IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE DEFAULT NULL,
                   I_order_no             IN  ORDHEAD.ORDER_NO%TYPE DEFAULT NULL,
                   I_option_id            IN  MA_STG_ORDER_OPTION.OPTION_ID%TYPE DEFAULT NULL,
                   I_po_status            IN  MA_STG_ORDER.STATUS%TYPE DEFAULT NULL,
                   I_supplier             IN  MA_STG_ORDER.SUPPLIER%TYPE DEFAULT NULL,
                   I_exp_delivery_date    IN  VARCHAR2 DEFAULT NULL,
                   I_created_by           IN  MA_STG_ORDER.CREATE_ID%TYPE DEFAULT NULL,
                   I_fc                   IN  MA_STG_ORDER_DROPS.FIRST_DEST%TYPE DEFAULT NULL,
                   I_po_type              IN  MA_STG_ORDER_ITEM_DIST.PO_TYPE%TYPE DEFAULT NULL,
                   I_po_source            IN  VARCHAR2 DEFAULT NULL,
                   I_handover_date        IN  VARCHAR2 DEFAULT NULL,
                   I_dept_list            IN  VARCHAR2 DEFAULT NULL,
                   I_class_list           IN  VARCHAR2 DEFAULT NULL,
                   I_subclass_list        IN  VARCHAR2 DEFAULT NULL,
                   I_business_model       IN  MA_V_UDA_ITEM_FF.UDA_TEXT%TYPE DEFAULT NULL,
                   I_business_model_list  IN  VARCHAR2 DEFAULT NULL,
                   I_buying_group_key     IN  VARCHAR2 DEFAULT NULL,
                   I_buying_group_list    IN  VARCHAR2 DEFAULT NULL,
                   I_buying_subgroup_list IN  VARCHAR2 DEFAULT NULL,
                   I_buying_set_list      IN  VARCHAR2 DEFAULT NULL,
                   I_process_id           IN  VARCHAR2 DEFAULT NULL,
                   I_process_date         IN  DATE DEFAULT NULL,
                   I_execute              IN  VARCHAR2 DEFAULT 'N',
                   I_factory              IN  MA_STG_ORDER_OPTION.FACTORY%TYPE DEFAULT NULL
                   )
RETURN PoSearchTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION GET_NEXT_ORDER_NBR (O_error_message OUT VARCHAR2)
RETURN NUMBER;
--------------------------------------------------------------------------------
FUNCTION GET_FINAL_DEST_SHIP_METHOD (O_error_message          OUT VARCHAR2,
                                     I_first_dest             IN  MA_STG_ORDER_ITEM_DIST.FIRST_DEST%TYPE, 
                                     I_final_dest             IN  MA_STG_ORDER_ITEM_DIST.FINAL_DEST%TYPE,
                                     O_ship_method_final_dest OUT MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE)
RETURN BOOLEAN;

--------------------------------------------------------------------------------
FUNCTION GET_DATE (O_error_message          OUT VARCHAR2,
                   I_factory                IN     MA_STG_ORDER_OPTION.FACTORY%TYPE,
                   I_po_type                IN     MA_STG_ORDER_ITEM_DIST.PO_TYPE%TYPE,
                   I_ship_port              IN     MA_STG_ORDER_ITEM_DIST.SHIP_PORT%TYPE,
                   I_first_dest             IN     MA_STG_ORDER_ITEM_DIST.FIRST_DEST%TYPE, 
                   I_ship_method            IN     MA_STG_ORDER_ITEM_DIST.SHIP_METHOD%TYPE, 
                   I_freight_forwarder      IN     MA_STG_ORDER_ITEM_DIST.FREIGHT_FORWARD%TYPE, 
                   I_final_dest             IN     MA_STG_ORDER_ITEM_DIST.FINAL_DEST%TYPE, 
                   I_ship_method_final_dest IN     MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE,             
                   IO_handover_date         IN OUT MA_STG_ORDER_ITEM_DIST.HANDOVER_DATE%TYPE,
                   IO_ship_date             IN OUT MA_STG_ORDER_ITEM_DIST.SHIP_DATE%TYPE,
                   IO_not_before_date       IN OUT MA_STG_ORDER_ITEM_DIST.NOT_BEFORE_DATE%TYPE,
                   IO_not_after_date        IN OUT MA_STG_ORDER_ITEM_DIST.NOT_AFTER_DATE%TYPE,
                   IO_first_dest_date       IN OUT MA_STG_ORDER_ITEM_DIST.FIRST_DEST_DATE%TYPE,  
                   IO_final_dest_date       IN OUT MA_STG_ORDER_ITEM_DIST.FINAL_DEST_DATE%TYPE,
                   O_ex_factory_date        OUT    MA_STG_ORDER_ITEM_DIST.EX_FACTORY_DATE%TYPE,
                   O_week_no                OUT    MA_STG_ORDER_ITEM_DIST.WEEK_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CHECK_QTY (O_error_message OUT VARCHAR2,
                    I_option_id     IN  MA_STG_ORDER_ITEM_DIST.OPTION_ID%TYPE,
                    I_supplier      IN  MA_STG_ORDER.SUPPLIER%TYPE,
                    I_qty_ordered   IN  MA_STG_ORDER_ITEM_DIST.QTY_ORDERED%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CREATE_ITEM_DIST (O_error_message   OUT VARCHAR2,
                           I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CREATE_DROP_DIST (O_error_message   OUT VARCHAR2,
                           I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE,
                           I_option_drop     IN  VARCHAR2 DEFAULT 'Y',
                           I_option_id       IN  MA_STG_ORDER_DROPS_DETAIL.OPTION_ID%TYPE,
                           I_new_qty         IN  MA_STG_ORDER_DROPS_DETAIL.QTY_ORDERED%TYPE DEFAULT 0)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CREATE_SIZING_DETAILS (O_error_message   OUT VARCHAR2,
                                I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CREATE_SIZING_SKU (O_error_message     OUT VARCHAR2,
                            I_seq_no            IN  MA_STG_SIZING_SKU.SEQ_NO%TYPE,
                            I_master_order_no  IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE,
                            I_order_no         IN  MA_STG_SIZING_SKU.ORDER_NO%TYPE,
                            I_option_id         IN  MA_STG_SIZING_SKU.OPTION_ID%TYPE,
                            I_final_dest        IN  MA_STG_SIZING_SKU.FINAL_DEST%TYPE,
                            I_exp_delivery_date IN  MA_STG_SIZING_SKU.EXP_DELIVERY_DATE%TYPE,
                            I_size_group_id     IN  MA_STG_ITEM_HEAD.DIFF_2_GROUP%TYPE,
                            I_profile_id        IN  MA_SIZE_PROFILE_DETAIL.SIZE_PROFILE%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CREATE_SIZING_SKU_FC (O_error_message    OUT VARCHAR2,
                               I_master_order_no IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE,
                               I_order_no        IN  MA_STG_SIZING_SKU.ORDER_NO%TYPE,
                               I_option_id        IN  MA_STG_SIZING_SKU.OPTION_ID%TYPE,  
                               I_size_group       IN  MA_STG_SIZING_OPTION_DIST.SIZE_GROUP%TYPE,                                                         
                               I_final_dest       IN  MA_STG_SIZING_SKU.FINAL_DEST%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION GENERATE_SKU (I_master_order_no IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE,
                       I_option_id        IN  MA_STG_SIZING_SKU.OPTION_ID%TYPE,
                       I_size_code        IN  MA_STG_SIZING_SKU.SIZE_CODE%TYPE)
RETURN MA_STG_SIZING_SKU.SKU%TYPE;
--------------------------------------------------------------------------------
FUNCTION PUB_ORDER_SIZING_SKUS(O_error_message    OUT VARCHAR2,
                               I_master_order_no IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION UPDATE_APPROVE_ORDER(O_error_message    OUT VARCHAR2,
                              I_master_order_no  IN  MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE,
                              I_order_no         IN  MA_STG_ORDER_DROPS.ORDER_NO%TYPE,
                              I_option_id        IN  MA_STG_ORDER_DROPS.OPTION_ID%TYPE,
                              I_unit_cost        IN  MA_STG_ORDER_DROPS.UNIT_COST%TYPE,
                              I_factory          IN  MA_STG_ORDER_OPTION.FACTORY%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CHECK_APPROVAL_LIMIT(O_error_message   OUT VARCHAR2,
                              I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE,
                              I_role            IN  MA_PO_APPROVAL_LIMIT_DETAIL.ROLE_ID%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
PROCEDURE DEQUEUE_PO_MASS_MNT_CALLBACK (context  RAW,
                                        reginfo  SYS.AQ$_REG_INFO,
                                        descr    SYS.AQ$_DESCRIPTOR,
                                        payload  RAW,
                                        payloadl NUMBER);
--------------------------------------------------------------------------------
FUNCTION PRC_PO_MASS_MNT_QUEUE_MESSAGE(O_error_message   OUT VARCHAR2,
                                       I_master_order_no IN  MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE,
                                       I_order_no        IN  MA_STG_ORDER_DROPS.ORDER_NO%TYPE,
                                       I_message_type    IN  MA_ORDER_MFQUEUE.MESSAGE_TYPE%TYPE,
                                       I_mass_mnt_user   IN  MA_STG_UPLOAD_PROCESS_LINE_IDS.CREATE_ID%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION ENQUEUE_PO_MASS_MNT_PROCESS (O_error_message   OUT VARCHAR2,
                                      I_master_order_no IN  MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE,
                                      I_order_no        IN  MA_STG_ORDER_DROPS.ORDER_NO%TYPE,
                                      I_message_type    IN  MA_ORDER_MFQUEUE.MESSAGE_TYPE%TYPE,
                                      I_mass_mnt_user   IN  MA_STG_UPLOAD_PROCESS_LINE_IDS.CREATE_ID%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------                              
FUNCTION MASS_MAINTENANCE_UPDATE(O_error_message       OUT VARCHAR2,
                                 I_order_no_tbl        IN  MA_ORDER_NO_TBL,
                                 I_exp_handover_date   IN  MA_STG_ORDER_DROPS.HANDOVER_DATE%TYPE DEFAULT NULL,
                                 I_handover_date_start IN  MA_STG_ORDER_DROPS.HANDOVER_DATE%TYPE DEFAULT NULL,
                                 I_handover_date_end   IN  MA_STG_ORDER_DROPS.HANDOVER_DATE%TYPE DEFAULT NULL,
                                 O_order_list          OUT MA_ORDER_NO_TBL)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CALCULATE_SIZING_QTY (O_error_message     OUT VARCHAR2,
                               I_dist_by           IN  VARCHAR2,
                               I_master_order_no   IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE,
                               I_order_no          IN  MA_STG_SIZING_SKU.ORDER_NO%TYPE,
                               I_option_id         IN  MA_STG_ORDER_ITEM_DIST.OPTION_ID%TYPE,
                               I_final_dest        IN  MA_STG_ORDER_DROPS.FINAL_DEST%TYPE,
                               I_exp_delivery_date IN  MA_STG_SIZING_SKU.EXP_DELIVERY_DATE%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION DELETE_STG_ORDER_TABLES(O_error_message    OUT VARCHAR2,
                                 I_master_order_no IN MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CALC_COST_SUMMARY(O_error_message    OUT VARCHAR2,
                           I_master_order_no IN MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CREATE_ORDER_REC_PLANNING (O_error_message    OUT VARCHAR2,
                                    I_order_rec_no_ids IN  VARCHAR2,
                                    O_master_order_no  OUT MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CREATE_ORDER_REC_REPLENISHMENT (O_error_message            OUT VARCHAR2,
                                         I_ma_order_rec_rpl_lst_tbl IN  MA_ORDER_REC_RPL_LST_TBL,
                                         O_master_order_no          OUT MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION CHECK_IF_ORDER_COMPLETE (O_error_message OUT VARCHAR2, 
                                  I_MASTER_ORDER_NO IN MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN NUMBER;
--------------------------------------------------------------------------------
FUNCTION LOAD_ORDER_TO_STG (O_error_message    OUT    VARCHAR2,
                            I_get_type         IN     VARCHAR2,
                            IO_master_order_no IN OUT MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_ORDER_OPTION(I_master_order_no IN MA_STG_ORDER_OPTION.MASTER_ORDER_NO%TYPE)
RETURN OrderOptionInfoObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_ORDER(I_master_order_no IN MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN OrderInfoObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_ORDER_DROPS(I_master_order_no IN MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE)
RETURN OrderDropsObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_ORDER_DROPS_DETAIL(I_master_order_no IN MA_STG_ORDER_DROPS_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderDropsDetailObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_SIZING_OPTION(I_master_order_no IN MA_STG_SIZING_OPTION_DIST.MASTER_ORDER_NO%TYPE)
RETURN OrderSizingOptionObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_SIZING_SKU(I_master_order_no IN MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE)
RETURN OrderSizingSkuObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_SIZING_DETAILS(I_master_order_no IN MA_V_SIZING_DETAILS.MASTER_ORDER_NO%TYPE)
RETURN OrderSizingDetailsObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_DROP_DETAIL(I_master_order_no IN Ma_STG_COST_DROP_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostDropDetailObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_DUTY_DETAIL(I_master_order_no IN MA_STG_COST_DUTY_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostDutyDetailObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_EXPENSE_DETAIL(I_master_order_no IN MA_STG_COST_EXPENSE_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostExpenseDetailObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_OPTION_DETAIL(I_master_order_no IN MA_STG_COST_OPTION_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostOptionDetailObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_UP_CHARGE_DETAIL(I_master_order_no IN MA_STG_COST_UP_CHARGE_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostUpChargeDetailObjTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION REPLENISHMENT_SEARCH(I_option_id            IN  MA_STG_ORDER_OPTION.OPTION_ID%TYPE DEFAULT NULL,
                              I_supplier             IN  MA_STG_ORDER.SUPPLIER%TYPE DEFAULT NULL,
                              I_fc                   IN  MA_STG_ORDER_DROPS.FIRST_DEST%TYPE DEFAULT NULL,
                              I_dept_list            IN  VARCHAR2 DEFAULT NULL,
                              I_class_list           IN  VARCHAR2 DEFAULT NULL,
                              I_subclass_list        IN  VARCHAR2 DEFAULT NULL,
                              I_business_model_list  IN  VARCHAR2 DEFAULT NULL,
                              I_buying_group_list    IN  VARCHAR2 DEFAULT NULL,
                              I_handover_date        IN  VARCHAR2 DEFAULT NULL,
                              I_execute_query        IN  VARCHAR2 DEFAULT 'N')
RETURN PoReplenSearchTbl PIPELINED;
--------------------------------------------------------------------------------
FUNCTION PLANNING_SEARCH( I_option_id                 IN  MA_STG_ORDER_OPTION.OPTION_ID%TYPE DEFAULT NULL,
                          I_supplier                  IN  MA_STG_ORDER.SUPPLIER%TYPE DEFAULT NULL,
                          I_fc                        IN  MA_STG_ORDER_DROPS.FIRST_DEST%TYPE DEFAULT NULL,
                          I_dept_list                 IN  VARCHAR2 DEFAULT NULL,
                          I_class_list                IN  VARCHAR2 DEFAULT NULL,
                          I_subclass_list             IN  VARCHAR2 DEFAULT NULL,
                          I_business_model_list       IN  VARCHAR2 DEFAULT NULL,
                          I_buying_group_list         IN  VARCHAR2 DEFAULT NULL,
                          I_execute_query             IN  VARCHAR2 DEFAULT 'N')
RETURN PoPlannSearchTbl PIPELINED;
--------------------------------------------------------------------------------
END MA_ORDER_UTILS_SQL_LHB;