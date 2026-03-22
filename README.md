# Тестовое задание

1.       Написать Hook к postgres, задать connection в интерфейсе airflow, обратиться в retail_dwh/dags/load_superstore_to_raw_superstore.py at main · Deni1006/retail_dwh · GitHub к connection через созданный hook и сделать подключение к базе данных.

2.       Сделать свой оператор Apache Airflow, получающий на вход:

-          Путь к файлу csv

-          Connection к бд postgres

-          Название таблицы

И выполняющий операцию чтения файла и запись в бд (то, что сейчас делает PythonOperator  в таске"load_to_bronze"). В таске "load_to_bronze" обратиться к созданному оператору вместо Python Operator.

3.       Описать преимущества использования оператора вместо функции.

4.       Прочесть статью во вложении, кратко описать – что такое проектирование хранилища данных в методологии Anchor Modeling.

5.       Описать назначение каждого из слоёв хранилища данных (RAW, ODS, MARTS).

6.       Есть запрос к бд Greenplum (Кластер postgres, состоящий из master, stanby- master, 4 сервера-сегментов. На каждом сервере сегментов по 2 сегмента без зеркалирования)

Почитать про Greenplum

Рассмотреть запрос к таблицам:

Таблица CM_NEW_ODN_DATA – 2769615377 строк DISTRIBUTED BY (entity_id)

Таблица cm_code_combinations – 341 строка DISTRIBUTED BY (code_combination_id)

Таблица ci_bseg_SQ - 472228768 строк DISTRIBUTED BY (bseg_id, uom_cd, tou_cd, sqi_cd)

 

                                SELECT

                                    regexp_replace(t.REFERENCE25, '[^0-9]*', '', 'g') AS bseg_id,

                                    entity_id,

                                    t.REFERENCE17 tu,

                                    t.REFERENCE26 rdo,

                                    (

                                        SELECT

                                            BILL_SQ

                                        FROM

                                            ci_bseg_SQ

                                        WHERE

                                            rtrim(SQI_CD) = 'SUMPOTR2'

                                            AND rtrim(UOM_CD) = 'GCAL'

                                            AND BSEG_ID = regexp_replace(t.REFERENCE25, '[^0-9]*', '', 'g')

                                    ) AS "Распр Отоп_cur",

                                    (

                                        SELECT

                                            init_SQ

                                        FROM

                                            ci_bseg_SQ

                                        WHERE

                                            rtrim(SQI_CD) = 'SUMPOTR2'

                                            AND rtrim(UOM_CD) = 'GCAL'

                                            AND BSEG_ID = regexp_replace(t.REFERENCE25, '[^0-9]*', '', 'g')

                                    ) AS "Распр Отоп_нач_1",

                                    (

                                        SELECT

                                            BILL_SQ

                                        FROM

                                            ci_bseg_SQ

                                        WHERE

                                            rtrim(SQI_CD) = 'SUMVDODN'

                                            AND rtrim(UOM_CD) = 'GCAL'

                                            AND BSEG_ID = regexp_replace(t.REFERENCE25, '[^0-9]*', '', 'g')

                                    ) AS "Вычет Отоп_cur",

                                    CASE

                                        WHEN (

                                            SELECT

                                                BILL_SQ

                                            FROM

                                                ci_bseg_SQ

                                            WHERE

                                                rtrim(SQI_CD) = 'SUMVDODN'

                                                AND rtrim(UOM_CD) = 'GCAL'

                                                AND BSEG_ID = regexp_replace(t.REFERENCE25, '[^0-9]*', '', 'g')

                                        ) IS NULL THEN (

                                            SELECT

                                                BILL_SQ

                                            FROM

                                                ci_bseg_SQ

                                            WHERE

                                                rtrim(SQI_CD) = 'SUMPOTR2'

                                                AND rtrim(UOM_CD) = 'GCAL'

                                                AND BSEG_ID = regexp_replace(t.REFERENCE25, '[^0-9]*', '', 'g')

                                        )

                                        ELSE (

                                            SELECT

                                                init_SQ

                                            FROM

                                                ci_bseg_SQ

                                            WHERE

                                                rtrim(SQI_CD) = 'SUMPOTR2'

                                                AND rtrim(UOM_CD) = 'GCAL'

                                                AND BSEG_ID = regexp_replace(t.REFERENCE25, '[^0-9]*', '', 'g')

                                        )

                                    END AS "Распр Отоп_нач_2"

                                FROM

                                    CM_NEW_ODN_DATA t

                                    JOIN bo_raw_ccb.cm_code_combinations ccc ON ccc.CODE_COMBINATION_ID = t.CODE_COMBINATION_ID

                                    AND ccc.SEGMENT1 = 'cmp'

                                    AND ccc.SEGMENT2 = 'd'

 

Указать проблемные места. Предложить вариант оптимизации запроса всеми возможными способами. Описать свою логику.
