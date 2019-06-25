// Databricks notebook source
dbutils.fs.put("/tmp/accounts.json", """
{"id": 1, "organization_id": 1},
{"id": 2, "organization_id": 1},
{"id": 3, "organization_id": 2}
""", true)

dbutils.fs.put("/tmp/irena/apps.json", """
{"account_id": 1, "user_id": 1},
{"account_id": 1, "user_id": 3},
{"account_id": 2, "user_id": 1},
{"account_id": 3, "user_id": 2}
""", true)

dbutils.fs.put("/tmp/irena/feature_settings.json", """
{"access_level":1,"feature_id":1, "level":0, "key": "max_allowed_users", "value": "--- 3"},
{"access_level":1,"feature_id":3, "level":0, "key": "settings", "value": ":settings:"},
{"access_level":1,"feature_id":4, "level":0, "key": "color", "value": "blue"},
{"access_level":1,"feature_id":4, "level":1, "key": "color", "value": "white"},
{"access_level":1,"feature_id":1, "level":31, "key": "max_allowed_users", "value": "--- 4"}
""", true)

dbutils.fs.put("/tmp/irena/features.json", """
{"name": "MultiLogin", "created_at": "2017-11-13 09:01:42", "updated_at": "2018-02-19 10:17:21", "owner_type": "Organization", "default_user_enabled": 1 , "id": 1, "feature_key": "ff376e27-a987-4c3e-b916-bfcb0245ff0b"},
{"name": "LiveChat", "created_at": "2017-11-13 09:01:42", "updated_at": "2018-02-19 10:17:21", "owner_type": "User", "default_user_enabled": 1, "id": 2, "feature_key": "55d46f31-c891-4319-9405-b2efeaad2c17"},
{"name": "ShoppableInstagram", "created_at": "2017-11-13 09:01:42", "updated_at": "2018-02-19 10:17:21", "owner_type": "Account", "default_user_enabled": 1, "id": 3, "feature_key": "fb95eb80-8c8d-4340-8ab6-1eb21c5db7c6"},
{"name": "CssEditor", "created_at": "2017-11-13 09:01:42", "updated_at": "2018-02-19 10:17:21", "owner_type": "Account", "default_user_enabled": 0, "id": 4, "feature_key": "5f7b65a0-eba6-49e0-9deb-4790d6640194"}
{"name": "whatEver", "created_at": "2018-10-13 09:01:42", "updated_at": "2018-10-19 10:17:21", "owner_type": "Organization", "default_user_enabled": 0, "id": 9999, "feature_key": "490a557a-513b-471b-9c9e-28f49ff650e5"}
""", true)

dbutils.fs.put("/tmp/irena/organizations.json", """
{"id": 1 , "organization_key": 1, "name": "yotpo-w2"}
{"id": 2, "organization_key": 2, "name": "Yotpo Web"}
""", true)

dbutils.fs.put("/tmp/irena/owner_feature_settings.json", """
{"owners_feature_id":1, "key":"max_allowed_users", "value":"5", "floating": true}
{"owners_feature_id":2, "key":"cool_setting", "value":"3", "floating": false}
""", true)

dbutils.fs.put("/tmp/irena/owners_packages.json", """
{"owner_id": 1, "owner_type": "Organization", "package_id": 1},
{"owner_id": 2, "owner_type": "Organization", "package_id": 1},
{"owner_id": 3, "owner_type": "Organization", "package_id": 1},

""", true)

dbutils.fs.put("/tmp/irena/owners_features.json", """
{"id": 1, "feature_id":1, "owner_type": "Account" , "owner_id":1, "user_enabled": 1, "disabled": 0, "floating": null},
{"id": 2, "feature_id":3, "owner_type": "Organization", "owner_id":1, "user_enabled": 1, "disabled": 0, "floating": null},
{"id": 3, "feature_id":9999, "owner_type": "Organization", "owner_id":1, "user_enabled": 1, "disabled": 0, "floating": null}
{"id": 3, "feature_id":4, "owner_type": "Account","owner_id":1, "user_enabled": 1,"disabled": 0, "floating": null}
""", true)

dbutils.fs.put("/tmp/irena/package_features.json", """
{"feature_key": "ff376e27-a987-4c3e-b916-bfcb0245ff0b", "promoted":false, "package_id":1},
{"feature_key": "ff376e27-a987-4c3e-b916-bfcb0245ff0b", "promoted":false, "package_id":2},
{"feature_key": "55d46f31-c891-4319-9405-b2efeaad2c17", "promoted":false, "package_id":1},
{"feature_key": "55d46f31-c891-4319-9405-b2efeaad2c17", "promoted":false, "package_id":2},
{"feature_key": "fb95eb80-8c8d-4340-8ab6-1eb21c5db7c6", "promoted":false, "package_id":1},
{"feature_key": "5f7b65a0-eba6-49e0-9deb-4790d6640194", "promoted":false, "package_id":1}
""", true)

dbutils.fs.put("/tmp/irena/packages.json", """
{"id": 1},
{"id": 2}
""", true)

dbutils.fs.put("/tmp/irena/privilege_types.json", """
{"id": 1, "name": "super_user"},
{"id": 2, "name": "nub_user"},
{"id": 3, "name": "developer"}
""", true)





// COMMAND ----------



// COMMAND ----------

spark.read.json("/tmp/accounts.json").createOrReplaceTempView("accounts")
spark.read.json("/tmp/irena/apps.json").createOrReplaceTempView("apps")
spark.read.json("/tmp/irena/feature_settings.json").createOrReplaceTempView("feature_settings")
spark.read.json("/tmp/irena/features.json").createOrReplaceTempView("features")
spark.read.json("/tmp/irena/organizations.json").createOrReplaceTempView("organizations")
spark.read.json("/tmp/irena/owner_feature_settings.json").createOrReplaceTempView("owner_feature_settings")
spark.read.json("/tmp/irena/owners_packages.json").createOrReplaceTempView("owners_packages")
spark.read.json("/tmp/irena/owners_features.json").createOrReplaceTempView("owners_features")
spark.read.json("/tmp/irena/package_features.json").createOrReplaceTempView("package_features")
spark.read.json("/tmp/irena/packages.json").createOrReplaceTempView("packages")
spark.read.json("/tmp/irena/privilege_types.json").createOrReplaceTempView("privilege_types")



// COMMAND ----------

// MAGIC %sql select * from accounts

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC create or replace temp view un_normalized_owners_features as 
// MAGIC SELECT apps.user_id,
// MAGIC        apps.account_id AS store_id,
// MAGIC        accounts.organization_id,
// MAGIC        organizations.organization_key,
// MAGIC        owners_features.id AS owner_feature_id,
// MAGIC        owners_features.feature_id,
// MAGIC        owners_features.owner_type AS feature_owner_type,
// MAGIC        owners_features.user_enabled AS is_feature_enabled_by_user,
// MAGIC        owners_features.disabled AS is_feature_disabled,
// MAGIC        floating
// MAGIC FROM apps
// MAGIC INNER JOIN accounts
// MAGIC       ON accounts.id = apps.account_id
// MAGIC INNER JOIN owners_features
// MAGIC       ON owners_features.owner_id = apps.user_id
// MAGIC INNER JOIN organizations
// MAGIC       ON accounts.organization_id = organizations.id
// MAGIC WHERE LOWER(owners_features.owner_type) = 'user'
// MAGIC UNION ALL
// MAGIC SELECT apps.user_id,
// MAGIC        apps.account_id AS store_id,
// MAGIC        accounts.organization_id,
// MAGIC        organizations.organization_key,
// MAGIC 	     owners_features.id AS owner_feature_id,
// MAGIC 	     owners_features.feature_id,
// MAGIC 	     owners_features.owner_type AS feature_owner_type,
// MAGIC 	     owners_features.user_enabled AS is_feature_enabled_by_user,
// MAGIC 	     owners_features.disabled AS is_feature_disabled,
// MAGIC        floating
// MAGIC FROM apps
// MAGIC INNER JOIN accounts
// MAGIC       ON accounts.id = apps.account_id
// MAGIC INNER JOIN owners_features
// MAGIC 	  ON owners_features.owner_id = apps.account_id
// MAGIC INNER JOIN organizations
// MAGIC       ON accounts.organization_id = organizations.id
// MAGIC WHERE LOWER(owners_features.owner_type) = 'account'
// MAGIC UNION ALL
// MAGIC SELECT apps.user_id,
// MAGIC 	     apps.account_id AS store_id,
// MAGIC        accounts.organization_id,
// MAGIC        organizations.organization_key,
// MAGIC 	     owners_features.id AS owner_feature_id,
// MAGIC 	     owners_features.feature_id,
// MAGIC 	     owners_features.owner_type AS feature_owner_type,
// MAGIC 	     owners_features.user_enabled AS is_feature_enabled_by_user,
// MAGIC 	     owners_features.disabled AS is_feature_disabled,
// MAGIC        floating
// MAGIC FROM apps
// MAGIC INNER JOIN accounts
// MAGIC       ON accounts.id = apps.account_id
// MAGIC INNER JOIN owners_features
// MAGIC 	  ON owners_features.owner_id = accounts.organization_id
// MAGIC INNER JOIN organizations
// MAGIC       ON accounts.organization_id = organizations.id
// MAGIC WHERE LOWER(owners_features.owner_type) = 'organization'

// COMMAND ----------

// MAGIC %sql select * from un_normalized_owners_features

// COMMAND ----------

// MAGIC %sql select * from features

// COMMAND ----------

// MAGIC %sql select * from owners_packages

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temp view user_store_features as
// MAGIC SELECT un_normalized_owners_features.organization_id,
// MAGIC        un_normalized_owners_features.store_id,
// MAGIC        -- keep user_id only if its user feature
// MAGIC        CASE WHEN LOWER(un_normalized_owners_features.feature_owner_type) = 'user' THEN un_normalized_owners_features.user_id END AS user_id,
// MAGIC        owners_packages.package_id AS plan_id,
// MAGIC        un_normalized_owners_features.feature_id,
// MAGIC        feature_owner_type,
// MAGIC        CASE WHEN un_normalized_owners_features.is_feature_enabled_by_user=1 THEN 1 ELSE 0 END AS is_feature_enabled_by_user,
// MAGIC        CASE WHEN un_normalized_owners_features.is_feature_disabled=1 THEN 1 ELSE 0 END AS is_feature_disabled,
// MAGIC        features.name AS feature_name,
// MAGIC        un_normalized_owners_features.owner_feature_id,
// MAGIC        features.created_at AS feature_created_at,
// MAGIC        features.updated_at AS feature_updated_at,
// MAGIC        CASE WHEN features.default_user_enabled=1 THEN 1 ELSE 0 END AS is_feature_enabled_by_default,
// MAGIC        CASE WHEN LOWER(un_normalized_owners_features.feature_owner_type) = 'user' THEN 1 ELSE 0 END AS is_user_feature,
// MAGIC        CASE WHEN LOWER(un_normalized_owners_features.feature_owner_type) = 'account' THEN 1 ELSE 0 END AS is_store_feature,
// MAGIC        CASE WHEN LOWER(un_normalized_owners_features.feature_owner_type) = 'organization' THEN 1 ELSE 0 END AS is_organization_feature,
// MAGIC        CASE WHEN package_features.promoted = FALSE THEN 1 ELSE 0 END AS is_organic_feature,
// MAGIC        floating
// MAGIC FROM un_normalized_owners_features
// MAGIC INNER JOIN features
// MAGIC 	  ON features.id = un_normalized_owners_features.feature_id
// MAGIC -- Currently owner type on owner_package is Organization
// MAGIC -- Inner join since EVERY organization is linked to a package
// MAGIC INNER JOIN owners_packages
// MAGIC       ON owners_packages.owner_id = un_normalized_owners_features.organization_key AND
// MAGIC          LOWER(owners_packages.owner_type) = 'organization'
// MAGIC INNER JOIN packages
// MAGIC 	  ON packages.id = owners_packages.package_id
// MAGIC LEFT JOIN package_features
// MAGIC       ON package_features.feature_key = features.feature_key AND
// MAGIC          package_features.package_id = owners_packages.package_id
// MAGIC GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

// COMMAND ----------

// MAGIC %sql select * from user_store_features

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temp view user_store_features_settings as
// MAGIC SELECT
// MAGIC 	 user_store_features.organization_id AS organization_id
// MAGIC     ,default_feature_settings.key AS feature_setting_key
// MAGIC 	,user_store_features.feature_id AS feature_id
// MAGIC 	,user_store_features.store_id AS store_id
// MAGIC     ,CASE WHEN user_store_features.floating = TRUE THEN 1 WHEN user_store_features.floating = FALSE THEN 0 ELSE NULL
// MAGIC      END AS is_floating_feature
// MAGIC 	,current_timestamp() AS dwh_updated_at
// MAGIC     ,concat_ws(",", collect_set(CAST(user_store_features.user_id AS string))) AS feature_store_setting_users
// MAGIC 	,MAX(user_store_features.plan_id) AS plan_id
// MAGIC     ,MAX(user_store_features.feature_owner_type) AS feature_owner_type
// MAGIC 	,MAX(user_store_features.is_user_feature) AS is_user_feature
// MAGIC 	,MAX(user_store_features.is_store_feature) AS is_store_feature
// MAGIC 	,MAX(user_store_features.is_organization_feature) AS is_organization_feature
// MAGIC 	,MAX(user_store_features.is_feature_enabled_by_user) AS is_feature_enabled_by_user
// MAGIC 	,MAX(user_store_features.is_feature_disabled) AS is_feature_disabled
// MAGIC 	,MAX(user_store_features.feature_name) AS feature_name
// MAGIC 	,MAX(user_store_features.is_feature_enabled_by_default) AS is_feature_enabled_by_default
// MAGIC 	,MAX(COALESCE(user_store_features.is_organic_feature,0)) AS is_organic_feature
// MAGIC 	,MAX(user_store_features.feature_created_at) AS feature_created_at
// MAGIC 	,MAX(user_store_features.feature_updated_at) AS feature_updated_at
// MAGIC 	,MAX(user_store_features.owner_feature_id) AS owner_feature_id
// MAGIC 	,MAX(privilege_types.name) AS privilege_type_name
// MAGIC 	,MAX(default_feature_settings.value) AS default_setting_value
// MAGIC 	,MAX(package_feature_settings.value) AS package_setting_value
// MAGIC 	,MAX(owner_feature_settings.value) AS owner_setting_value
// MAGIC 	,MAX(COALESCE(owner_feature_settings.value, package_feature_settings.value, default_feature_settings.value)) AS ultimate_setting_value
// MAGIC FROM user_store_features
// MAGIC LEFT JOIN feature_settings AS default_feature_settings
// MAGIC      ON user_store_features.feature_id = default_feature_settings.feature_id AND
// MAGIC         default_feature_settings.level = 0
// MAGIC         -- in order to set an owner features setting is level=0 on the feature_settings required?
// MAGIC         -- otherwise how do I link the setting key?
// MAGIC LEFT JOIN feature_settings AS package_feature_settings
// MAGIC      ON user_store_features.feature_id = package_feature_settings.feature_id  AND
// MAGIC         user_store_features.plan_id = package_feature_settings.level AND
// MAGIC         default_feature_settings.key = package_feature_settings.key AND
// MAGIC         package_feature_settings.level <> 0
// MAGIC LEFT JOIN owner_feature_settings
// MAGIC 	 ON owner_feature_settings.owners_feature_id=user_store_features.owner_feature_id AND
// MAGIC 	    default_feature_settings.key = owner_feature_settings.key AND
// MAGIC 	    owner_feature_settings.floating = TRUE
// MAGIC -- no feature settings at the extension level
// MAGIC -- privilege_types not relevant for package extensions
// MAGIC LEFT JOIN privilege_types
// MAGIC      ON COALESCE(package_feature_settings.access_level, default_feature_settings.access_level) = privilege_types.id
// MAGIC GROUP BY 1, 2, 3, 4, 5, 6

// COMMAND ----------

// MAGIC %sql select * from feature_settings

// COMMAND ----------

// MAGIC %sql select * from user_store_features_settings

// COMMAND ----------

