Scenario: Set Airflow configuration
When I set Airflow variable 'autotest_bsi_batch_date' with value '#{generateDate(P, YYYY-MM-dd)}'
When I set Airflow variable 'autotest_bsi_samples_schema' with value '2019-11-11'

Scenario: Register new BSI samples with parent_ids or poooled_parent_ids specified
When I clear DB with key 'hsm_autotest'
When I clear 'bsi' test data in all S3 buckets
When I generate parquet with the name `s3a://${rawBucket}/${bsiSamples}` using schema `${bsiSchemaPath}` and data:
`|sample_id     |parent_sample_id |pool_parent_sample_id                                                                                                                    |external_reference |barcode            |collaborator_vendor_type|freezer_building|freezer_room|freezer_id|freezer_name|freezer_shelf  |freezer_rack|freezer_box|freezer_row|freezer_col|sample_type  |subject_id|study_id     |sample_collection_date|subject_site_group|visit_date|designation|ship_in_date|ship_out_date|freeze_thaw_cycle_count|available_for_research|sample_condition|vial_status|volume |volume_unit|sample_consumed|sample_consumed_date|sample_disposed|sample_disposed_date|anticipated_destruction_date|sample_destroyed|sample_destroyed_date|number_of_donors|notes  |update_date          |
 |AAA004843 0030|AAA004843 0000   |null                                                                                                                                     |AO9524430 0030     |AO9524430 0030     |Regeneron Clinical Trial|32-270          |blank       |ID 69475  |ID 69475    |3 Lower Cabinet|069         |B          |002        |J          |Plasma (EDTA)|826001014 |R3918-HV-1659|2017-07-11 07:38:00.0 |826001            |null      |Biomarker  |null        |null         |3                      |null                  |Acceptable      |Reserved   |12.000 |ML         |false          |null                |true           |null                |null                        |false           |null                 |null            |C3a MQC|2019-11-22 14:47:00.0|
 |AAA004843 0031|AAA004843 0030   |null                                                                                                                                     |AO9524430 0031 0030|AO9524430 0031 0030|Regeneron Clinical Trial|32-270          |blank       |ID 69475  |ID 69475    |3 Lower Cabinet|069         |B          |002        |J          |Plasma (EDTA)|826001015 |R3918-HV-1660|2017-07-11 07:38:00.0 |826002            |null      |Biomarker  |null        |null         |3                      |null                  |Acceptable      |Reserved   |156.000|ML         |false          |null                |true           |null                |null                        |false           |null                 |null            |C3a MQC|2019-11-22 14:47:00.0|
 |AAA004843 0032|null             |[{\"parent_sample_id\": \"AAA004843 0031\"\}]                                                                                            |AO9524430 0032 0000|AO9524430 0032 0000|Regeneron Clinical Trial|32-272          |blank       |ID 69472  |ID 69472    |2 Lower Cabinet|072         |A          |002        |D          |Plasma (EDTA)|826001016 |R3918-HV-1661|2017-07-11 07:38:00.0 |826003            |null      |Biomarker  |null        |null         |3                      |null                  |Acceptable      |Reserved   |156.000|ML         |false          |null                |true           |null                |null                        |false           |null                 |null            |C3a MQC|2019-11-22 14:47:00.0|
 |AAA004843 0034|null             |[{\"parent_sample_id\": \"AAA004843 0031\"\}\, {\"parent_sample_id\": \"AAA004843 0032\"\}\, {\"parent_sample_id\": \"AAA004843 0033\"\}]|AO9524430 0034 0000|AO9524430 0034 0000|Regeneron Clinical Trial|32-271          |blank       |ID 69471  |ID 69471    |1 Lower Cabinet|071         |B          |001        |E          |Plasma (EDTA)|826001017 |R3918-HV-1662|2017-07-11 07:38:00.0 |826004            |null      |Biomarker  |null        |null         |3                      |null                  |Acceptable      |Reserved   |156.000|ML         |false          |null                |true           |null                |null                        |false           |null                 |null            |C3a MQC|2019-11-22 14:47:00.0|
 |AAA004843     |                 |                                                                                                                                         |AO9524430 0035     |AO9524430 0035     |Regeneron               |32-300          |blank       |ID 69300  |ID 69300    |2 Upper Cabinet|070         |C          |003        |F          |Plasma (EDTA)|826001018 |R3918-HV-1663|2017-07-11 07:38:00.0 |826005            |null      |Biomarker  |null        |null         |3                      |null                  |Acceptable      |Reserved   |156.000|ML         |false          |null                |true           |null                |null                        |false           |null                 |null            |C3a MQC|2019-11-22 14:47:00.0|`
When I perform Airflow DAG run 'autotest_hsm_samples_bsi'
!-- Get hsm_sample_ids of registered samples
When I execute SQL query `SELECT hsm_sample_id FROM sample_id WHERE sample_id='AAA004843 0000'` against `hsm_autotest` and save result to SCENARIO variable `rootSample1Id`
When I initialize the STORY variable `rootParent1Id` with value `#{replaceAllByRegExp(^(\[)(\{hsm_sample_id=(\d)\})(.*), $3, ${rootSample1Id})}`
When I execute SQL query `SELECT hsm_sample_id FROM sample_id WHERE sample_id='AAA004843'` against `hsm_autotest` and save result to SCENARIO variable `rootSample2Id`
When I initialize the STORY variable `rootParent2Id` with value `#{replaceAllByRegExp(^(\[)(\{hsm_sample_id=(\d)\})(.*), $3, ${rootSample2Id})}`
When I execute SQL query `SELECT hsm_sample_id FROM sample_id WHERE sample_id='AAA004843 0030'` against `hsm_autotest` and save result to SCENARIO variable `leaf1Id`
When I initialize the STORY variable `aliquot1Id` with value `#{replaceAllByRegExp(^(\[)(\{hsm_sample_id=(\d)\})(.*), $3, ${leaf1Id})}`
When I execute SQL query `SELECT hsm_sample_id FROM sample_id WHERE sample_id='AAA004843 0031'` against `hsm_autotest` and save result to SCENARIO variable `leaf2Id`
When I initialize the STORY variable `aliquot2Id` with value `#{replaceAllByRegExp(^(\[)(\{hsm_sample_id=(\d)\})(.*), $3, ${leaf2Id})}`
When I execute SQL query `SELECT hsm_sample_id FROM sample_id WHERE sample_id='AAA004843 0032'` against `hsm_autotest` and save result to SCENARIO variable `pooled1Id`
When I initialize the STORY variable `pooledSample1Id` with value `#{replaceAllByRegExp(^(\[)(\{hsm_sample_id=(\d)\})(.*), $3, ${pooled1Id})}`
When I execute SQL query `SELECT hsm_sample_id FROM sample_id WHERE sample_id='AAA004843 0034'` against `hsm_autotest` and save result to SCENARIO variable `pooled2Id`
When I initialize the STORY variable `pooledSample2Id` with value `#{replaceAllByRegExp(^(\[)(\{hsm_sample_id=(\d)\})(.*), $3, ${pooled2Id})}`

Scenario: Verify sample is registered as a child of sample with specified parent_id
When I execute SQL query `SELECT text(id) as id, coalesce(text(parent_id), 'null') as parent_id FROM sample ORDER BY id, parent_id;` against `hsm_autotest` and save result to SCENARIO variable `samples`
Then `${samples}` is equal to table:
|id                |parent_id         |
|${rootParent2Id}  |null              |
|${aliquot1Id}     |${rootParent1Id}  |
|${aliquot2Id}     |${aliquot1Id}     |
|${pooledSample1Id}|null              |
|${pooledSample2Id}|null              |
|${rootParent1Id}  |null              |

Scenario: Verify sample is registered as a pooled sample-child of all samples with specified pooled_parent_ids
When I execute SQL query `SELECT text(id) as id, text(parent_id) as parent_id FROM pooled_sample ORDER BY id, parent_id;` against `hsm_autotest` and save result to SCENARIO variable `pooledSamples`
Then `${pooledSamples}` is equal to table:
|id                |parent_id         |
|${pooledSample1Id}|${aliquot2Id}     |
|${pooledSample2Id}|${aliquot2Id}     |
|${pooledSample2Id}|${pooledSample1Id}|

Scenario: Verify surrogate samples are created for unknown parents
!-- Root sample is processed correctly though its parent id is specified as an empty string
!-- Surrogate created for unknown parent_id  (unknown pooled parents are not processed at the moment)
When I execute SQL query `SELECT text(hsm_sample_id) as hsm_sample_id, source_system, coalesce(text(label), 'null') as label, coalesce(text(external_reference), 'null') as external_reference, coalesce(text(study_id), 'null') as study_id, coalesce(text(subject_id), 'null') as subject_id, coalesce(text(subject_site_group), 'null') as subject_site_group FROM sample_var ORDER BY hsm_sample_id;` against `hsm_autotest` and save result to SCENARIO variable `sampleVarSamples`
Then `${sampleVarSamples}` is equal to table:
|hsm_sample_id     |source_system|label              |external_reference   |study_id        |subject_id |subject_site_group|
|${rootParent2Id}  |BSI	         |AO9524430 0035     |AO9524430 0035       |R3918-HV-1663	|826001018	|826005            |
|${aliquot1Id}     |BSI	         |AO9524430 0030     |AO9524430 0030	   |R3918-HV-1659	|826001014	|826001            |
|${aliquot2Id}     |BSI	         |AO9524430 0031 0030|AO9524430 0031 0030  |R3918-HV-1660	|826001015	|826002            |
|${pooledSample1Id}|BSI	         |AO9524430 0032 0000|AO9524430 0032 0000  |R3918-HV-1661	|826001016	|826003            |
|${pooledSample2Id}|BSI	         |AO9524430 0034 0000|AO9524430 0034 0000  |R3918-HV-1662	|826001017	|826004            |
|${rootParent1Id}  |BSI	         |null               |null                 |null        	|null    	|null              |

Scenario: Verify type of registered samples is tracked correctly
When I execute SQL query `SELECT dtype FROM mv_sample_search WHERE hsm_sample_id='${rootParent1Id}';` against `hsm_autotest` and save result to SCENARIO variable `rootParent1Type`
Then `${rootParent1Type}` is equal to table:
|dtype     |
|Sample    |
When I execute SQL query `SELECT dtype FROM mv_sample_search WHERE hsm_sample_id='${rootParent2Id}';` against `hsm_autotest` and save result to SCENARIO variable `rootParent2Type`
Then `${rootParent2Type}` is equal to table:
|dtype     |
|Sample    |
When I execute SQL query `SELECT dtype FROM mv_sample_search WHERE hsm_sample_id='${aliquot1Id}';` against `hsm_autotest` and save result to SCENARIO variable `aliquot1Type`
Then `${aliquot1Type}` is equal to table:
|dtype     |
|Aliquot(1)|
When I execute SQL query `SELECT dtype FROM mv_sample_search WHERE hsm_sample_id='${aliquot2Id}';` against `hsm_autotest` and save result to SCENARIO variable `aliquot2Type`
Then `${aliquot2Type}` is equal to table:
|dtype     |
|Aliquot(2)|
When I execute SQL query `SELECT dtype FROM mv_sample_search WHERE hsm_sample_id='${pooledSample1Id}';` against `hsm_autotest` and save result to SCENARIO variable `pooledSample1Type`
Then `${pooledSample1Type}` is equal to table:
|dtype     |
|Pooled    |
When I execute SQL query `SELECT dtype FROM mv_sample_search WHERE hsm_sample_id='${pooledSample2Id}';` against `hsm_autotest` and save result to SCENARIO variable `pooledSample2Type`
Then `${pooledSample2Type}` is equal to table:
|dtype     |
|Pooled    |

Scenario: Register new BSI samples with known surrogate parent
!-- Line 1: Record contains an update for created surrogate parrent to verify that surrogates are identified
!-- Line 2: Record contains a new pooled parent that relates to already created surrogate to verify a) duplicate surrogates are not created, b) previously added pooled parents are not removed, but combined with newly received data
When I clear test data in '${bsiRaw}' zone
When I generate parquet with the name `s3a://${rawBucket}/${bsiSamples}` using schema `${bsiSchemaPath}` and data:
`|sample_id     |parent_sample_id |pool_parent_sample_id                           |external_reference |barcode            |collaborator_vendor_type|freezer_building|freezer_room|freezer_id|freezer_name|freezer_shelf  |freezer_rack|freezer_box|freezer_row|freezer_col|sample_type  |subject_id|study_id     |sample_collection_date|subject_site_group|visit_date|designation|ship_in_date|ship_out_date|freeze_thaw_cycle_count|available_for_research|sample_condition|vial_status|volume |volume_unit|sample_consumed|sample_consumed_date|sample_disposed|sample_disposed_date|anticipated_destruction_date|sample_destroyed|sample_destroyed_date|number_of_donors|notes  |update_date          |
 |AAA004843 0000|null             |null                                            |AO9524430 0000     |AO9524430 0000     |Regeneron Clinical Trial|32-200          |blank       |ID 69400  |ID 69475    |3 Lower Cabinet|069         |B          |002        |J          |Plasma (EDTA)|826001000 |R3918-HV-1600|2017-01-01 00:00:00.0 |826001            |null      |Biomarker  |null        |null         |3                      |null                  |Acceptable      |Reserved   |12.000 |ML         |false          |null                |true           |null                |null                        |false           |null                 |null            |C3a MQC|2019-11-23 14:47:00.0|
 |AAA004843 0032|null             |[{\"parent_sample_id\": \"AAA004843 0000\"\}]   |AO9524430 0032 0000|AO9524430 0032 0000|Regeneron Clinical Trial|32-272          |blank       |ID 69472  |ID 69472    |2 Lower Cabinet|072         |A          |002        |D          |Plasma (EDTA)|826001016 |R3918-HV-1661|2017-07-11 07:38:00.0 |826003            |null      |Biomarker  |null        |null         |3                      |null                  |Acceptable      |Reserved   |156.000|ML         |false          |null                |true           |null                |null                        |false           |null                 |null            |C3a MQC|2019-11-23 14:47:00.0|`
When I perform Airflow DAG run 'autotest_hsm_samples_bsi'

Scenario: Verify created surrogates are identified and correctly updated
When I execute SQL query `SELECT text(hsm_sample_id) as hsm_sample_id, source_system, coalesce(text(label), 'null') as label, coalesce(text(external_reference), 'null') as external_reference, coalesce(text(study_id), 'null') as study_id, coalesce(text(subject_id), 'null') as subject_id, coalesce(text(subject_site_group), 'null') as subject_site_group FROM mv_sample_search WHERE hsm_sample_id=${rootParent1Id};` against `hsm_autotest` and save result to SCENARIO variable `updatedSurrogateRecord`
Then `${updatedSurrogateRecord}` is equal to table:
|hsm_sample_id     |source_system|label              |external_reference   |study_id        |subject_id |subject_site_group|
|${rootParent1Id}  |BSI	         |AO9524430 0000     |AO9524430 0000       |R3918-HV-1600   |826001000  |826001            |

Scenario: Verify previously added pooled parents are combined with newly received ones
When I execute SQL query `SELECT text(id) as id, text(parent_id) as parent_id FROM pooled_sample WHERE id='${pooledSample1Id}' ORDER BY parent_id;` against `hsm_autotest` and save result to SCENARIO variable `pooledSamples`
Then `${pooledSamples}` is equal to table:
|id                |parent_id         |
|${pooledSample1Id}|${aliquot2Id}     |
|${pooledSample1Id}|${rootParent1Id}  |
