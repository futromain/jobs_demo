# Databricks notebook source
# MAGIC %pip install iso3166 Faker

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters and cleanup
# MAGIC Let's use [widgets](https://docs.databricks.com/notebooks/widgets.html) to set dynamic parameters

# COMMAND ----------

dbutils.widgets.dropdown('reset_all_data', 'false', ['true', 'false'], 'Reset all existing data')
dbutils.widgets.combobox('num_recs', '1000', ['0', '1000', '3000'], 'Volume (# records per writes)')
dbutils.widgets.text('name', 'romain', 'Enter your name')

# COMMAND ----------

reset_all_data = dbutils.widgets.get('reset_all_data') == "true"
name = dbutils.widgets.get('name')
output_path = '/twill_demo/'+name+'/loans'

if reset_all_data:
  print(f'cleanup data {output_path}')
  dbutils.fs.rm(output_path, True)
dbutils.fs.mkdirs(output_path)
dbutils.fs.mkdirs(output_path+'/raw_transactions')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Generate random data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definitions

# COMMAND ----------

from faker import Faker
from collections import OrderedDict 
import pyspark.sql.functions as F
import uuid
import random

fake = Faker()
base_rates = OrderedDict([("ZERO", 0.5),("UKBRBASE", 0.1),("FDTR", 0.3),(None, 0.01)])
base_rate = F.udf(lambda:fake.random_elements(elements=base_rates, length=1)[0])
fake_country_code = F.udf(fake.country_code)

fake_date = F.udf(lambda:fake.date_time_between(start_date="-2y", end_date="+0y").strftime("%m-%d-%Y %H:%M:%S"))
fake_date_future = F.udf(lambda:fake.date_time_between(start_date="+0y", end_date="+2y").strftime("%m-%d-%Y %H:%M:%S"))
fake_date_current = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
def random_choice(enum_list):
  return F.udf(lambda:random.choice(enum_list))

fake.date_time_between(start_date="-10y", end_date="+30y")

def generate_transactions(num, folder, file_count, mode):
  (spark.range(0,num)
  .withColumn("acc_fv_change_before_taxes", (F.rand()*1000+100).cast('int'))
  .withColumn("purpose", (F.rand()*1000+100).cast('int'))

  .withColumn("accounting_treatment_id", (F.rand()*6).cast('int'))
  .withColumn("accrued_interest", (F.rand()*100+100).cast('int'))
  .withColumn("arrears_balance", (F.rand()*100+100).cast('int'))
  .withColumn("base_rate", base_rate())
  .withColumn("behavioral_curve_id", (F.rand()*6).cast('int'))
  .withColumn("cost_center_code", fake_country_code())
  .withColumn("country_code", fake_country_code())
  .withColumn("date", fake_date())
  .withColumn("end_date", fake_date_future())
  .withColumn("next_payment_date", fake_date_current())
  .withColumn("first_payment_date", fake_date_current())
  .withColumn("last_payment_date", fake_date_current())
  .withColumn("behavioral_curve_id", (F.rand()*6).cast('int'))
  .withColumn("count", (F.rand()*500).cast('int'))
  .withColumn("arrears_balance", (F.rand()*500).cast('int'))
  .withColumn("balance", (F.rand()*500-30).cast('int'))
  .withColumn("imit_amount", (F.rand()*500).cast('int'))
  .withColumn("minimum_balance_eur", (F.rand()*500).cast('int'))
  .withColumn("type", random_choice([
          "bonds","call","cd","credit_card","current","depreciation","internet_only","ira",
          "isa","money_market","non_product","deferred","expense","income","intangible","prepaid_card",
          "provision","reserve","suspense","tangible","non_deferred","retail_bonds","savings",
          "time_deposit","vostro","other","amortisation"
        ])())
  .withColumn("status", random_choice(["active", "cancelled", "cancelled_payout_agreed", "transactional", "other"])())
  .withColumn("guarantee_scheme", random_choice(["repo", "covered_bond", "derivative", "none", "other"])())
  .withColumn("encumbrance_type", random_choice(["be_pf", "bg_dif", "hr_di", "cy_dps", "cz_dif", "dk_gdfi", "ee_dgs", "fi_dgf", "fr_fdg",  "gb_fscs",
                                                 "de_edb", "de_edo", "de_edw", "gr_dgs", "hu_ndif", "ie_dgs", "it_fitd", "lv_dgf", "lt_vi",
                                                 "lu_fgdl", "mt_dcs", "nl_dgs", "pl_bfg", "pt_fgd", "ro_fgdb", "sk_dpf", "si_dgs", "es_fgd",
                                                 "se_ndo", "us_fdic"])())
  .withColumn("purpose", random_choice(['admin','annual_bonus_accruals','benefit_in_kind','capital_gain_tax','cash_management','cf_hedge','ci_service',
                    'clearing','collateral','commitments','computer_and_it_cost','corporation_tax','credit_card_fee','critical_service','current_account_fee',
                    'custody','employee_stock_option','dealing_revenue','dealing_rev_deriv','dealing_rev_deriv_nse','dealing_rev_fx','dealing_rev_fx_nse',
                    'dealing_rev_sec','dealing_rev_sec_nse','deposit','derivative_fee','dividend','div_from_cis','div_from_money_mkt','donation','employee',
                    'escrow','fees','fine','firm_operating_expenses','firm_operations','fx','goodwill','insurance_fee','intra_group_fee','investment_banking_fee',
                    'inv_in_subsidiary','investment_property','interest','int_on_bond_and_frn','int_on_bridging_loan','int_on_credit_card','int_on_ecgd_lending',
                    'int_on_deposit','int_on_derivative','int_on_deriv_hedge','int_on_loan_and_adv','int_on_money_mkt','int_on_mortgage','int_on_sft','ips',
                    'loan_and_advance_fee','ni_contribution','manufactured_dividend','mortgage_fee','non_life_ins_premium','occupancy_cost','operational',
                    'operational_excess','operational_escrow','other','other_expenditure','other_fs_fee','other_non_fs_fee','other_social_contrib',
                    'other_staff_rem','other_staff_cost','overdraft_fee','own_property','pension','ppe','prime_brokerage','property','recovery',
                    'redundancy_pymt','reference','reg_loss','regular_wages','release','rent','restructuring','retained_earnings','revaluation',
                    'revenue_reserve','share_plan','staff','system','tax','unsecured_loan_fee','write_off'])())
  ).repartition(file_count).write.format('json').mode(mode).save(folder)
  cleanup_folder(output_path+'/raw_transactions')
  
def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run generation

# COMMAND ----------

num_recs = int(dbutils.widgets.get('num_recs'))
#assert num_recs > 0, "There are no records to write!"

# COMMAND ----------

if num_recs > 0:
  generate_transactions(num_recs, output_path+'/raw_transactions', 10, "append")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Count how many files are available and pass it to the workflow

# COMMAND ----------

paths = dbutils.fs.ls(output_path+'/raw_transactions') 
file_count = len([p for p in paths])

print(f'There are {file_count} files')
dbutils.jobs.taskValues.set('file_count', file_count)

# COMMAND ----------

dbutils.jobs.taskValues.set('name', name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate some extra, static data

# COMMAND ----------

spark.read.csv('/databricks-datasets/lending-club-loan-stats', header=True) \
      .withColumn('id', F.monotonically_increasing_id()) \
      .withColumn('member_id', (F.rand()*1000000).cast('int')) \
      .withColumn('accounting_treatment_id', (F.rand()*6).cast('int')) \
      .repartition(50).write.mode('overwrite').option('header', True).format('csv').save(output_path+'/historical_loans')

spark.createDataFrame([
  (0, 'held_to_maturity'),
  (1, 'available_for_sale'),
  (2, 'amortised_cost'),
  (3, 'loans_and_recs'),
  (4, 'held_for_hedge'),
  (5, 'fv_designated')
], ['id', 'accounting_treatment']).write.format('delta').mode('overwrite').save(output_path + "/ref_accounting_treatment")
      
cleanup_folder(output_path+'/historical_loans')
cleanup_folder(output_path+'/ref_accounting_treatment')

# COMMAND ----------

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {name}')
