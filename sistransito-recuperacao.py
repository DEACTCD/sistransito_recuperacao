import utils
import sql

base = utils.SistransitoRecuperacao()

basedf = base.get_dataFrames()

print(basedf)

support = utils.SupportRules(basedf)

basedf2 = support.set_new_columns()

print(basedf2)

basedf3 = support.order_columns()

#support.excel_df()

send = sql.BancoSistransito()

send.def_temp_table()
send.set_temp_data(basedf3)
send.merge_sistransito()
