# Dynamics-by-account.-SQL-Advanced-Module-Task
Dynamics by account. SQL Advanced Module Task 
Опис завдання:
Для виконання завдання використовуй e-commerce базу даних, з якою ми працювали в BigQuery. Напиши один SQL-запит для створення набору даних. Додатково створи візуалізацію в Looker Studio на основі отриманих даних.
Необхідно зібрати дані, які допоможуть аналізувати динаміку створення акаунтів, активність користувачів за листами (відправлення, відкриття, переходи), а також оцінювати поведінку в категоріях, таких як інтервал відправлення, верифікація акаунтів і статус підписки. Дані дозволять порівнювати активність між країнами, визначати ключові ринки, сегментувати користувачів за різними параметрами.
Вимоги до SQL-запиту
Результат запиту повинен містити певний перелік полів групування - тобто категоріальних значень (з якими не проводиться ніяких обчислень). Основні метрики по акаунтам, а також основні метрики по емейлам, повинні розраховуватися в цих розрізах:
date — дата (для акаунтів - дата створення акаунта, для емейлів - дата відправки емейла);
country — країна;
send_interval — інтервал відправлення, встановлений акаунтом;
is_verified — перевірено акаунт чи ні;
is_unsubscribed — підписник відписався чи ні.
Твій запит має розраховувати інформацію (в розрізі перелічених полів) по наступним основним метриках:
account_cnt — кількість створених акаунтів;
sent_msg — кількість відправлених листів;
open_msg — кількість відкритих листів;
visit_msg — кількість переходів по листах;
А також по додатковим метриках (які розраховуються на базі основних метрик):
total_country_account_cnt — загальна кількість створених підписників в цілому по країні;
total_country_sent_cnt — загальна кількість відправлених листів в цілому по країні;
rank_total_country_account_cnt — рейтинг країн за кількістю створених підписників в цілому по країні;
rank_total_country_sent_cnt — рейтинг країн за кількістю відправлених листів в цілому по країні.
Метрики для акаунтів і емейлів потрібно розраховувати окремо, щоб зберегти унікальні розрізи для кожного та уникнути конфліктів через різну логіку використання поля date. Для об’єднання результатів використовуй UNION. У фіналі залиш лише ті записи, де rank_total_country_account_cnt або rank_total_country_sent_cnt менше або дорівнює 10.
Обов'язково використай хоча б одне CTE, винеси туди логічні частини запиту. Для розрахунку рангу використовуй функції-вікна.
Структура набору
Запит повинен виводити такі стовпчики:
date — дата;
country — країна;
send_interval — інтервал відправлення;
is_verified — перевірено акаунт чи ні;
is_unsubscribed — підписник відписався;
account_cnt — кількість створених акаунтів;
sent_msg — кількість відправлених листів;
open_msg — кількість відкритих листів;
visit_msg — кількість переходів по листах;
total_country_account_cnt — загальна кількість створених підписників по країні;
total_country_sent_cnt — загальна кількість відправлених листів по країні;
rank_total_country_account_cnt — рейтинг країн за кількістю створених підписників;
rank_total_country_sent_cnt — рейтинг країн за кількістю відправлених листів.
Візуалізація
Створи візуалізацію в Looker Studio, яка показуватиме загальні значення в розрізі країн для таких полів:
account_cnt;
total_country_sent_cnt;
rank_total_country_account_cnt;
rank_total_country_sent_cnt.
Покажи динаміку по даті для поля sent_msg.
Формат оформлення результатів
Документ: додай код SQL-запиту в документ. Додай коментарі до основних частин коду для пояснення логіки.
Скріншот: додай в документ скріншот створеної візуалізації в Looker Studio.
❗️ Посилання на документ з описом результатів прикріпи в завданні SQL Advanced Module Task.


Виконання:


WITH union_t -- Об'єднання двох таблиць по основним параметрам.
AS (
-- Підрахунок кількості акаунтів по даті, країні, інтервалу, веріфікаціі та підпискам. По умовам завдання повинно бути реалізовано через Юніон
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    COUNT (DISTINCT a.id) AS account_cnt,
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg,  


FROM
    `DA.account` a
    JOIN `DA.account_session` acs
    ON acs.account_id = a.id
    JOIN `DA.session` s
    ON s.ga_session_id = acs.ga_session_id
    JOIN `DA.session_params` sp
    ON sp.ga_session_id = s.ga_session_id


    GROUP BY 1,2,3,4,5


    UNION ALL
-- Підрахунок кількості відправлених, відкритих та клікнутих листів в розрізі даті, країні, інтервалу, веріфікаціі для Юніон.
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    0 AS account_cnt,
    COUNT (DISTINCT es.id_message) AS sent_msg,
    COUNT (DISTINCT eo.id_message) AS open_msg,
    COUNT (DISTINCT ev.id_message) AS visit_msg,


FROM
   `DA.account` a
    JOIN `DA.account_session` acs
    ON acs.account_id = a.id
    JOIN `DA.session` s
    ON s.ga_session_id = acs.ga_session_id
    JOIN `DA.session_params` sp
    ON sp.ga_session_id = s.ga_session_id
    LEFT JOIN `DA.email_sent` es
    ON es.id_account = a.id
    LEFT JOIN `DA.email_open` eo
    ON eo.id_message = es.id_message
    LEFT JOIN `DA.email_visit` ev
    ON ev.id_message = eo.id_message


GROUP BY 1,2,3,4,5),


sum_t -- Згрупувати, щоб не було задвоєння даних та пустих рядків після юніон
AS (
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM (account_cnt) as account_cnt,
    SUM (open_msg) as open_msg,
    SUM (visit_msg) as visit_msg,
    SUM (sent_msg) as  sent_msg,
FROM union_t
GROUP BY 1,2,3,4,5),
total_t -- Додавання віндовс функцій для підрахунку загальної кількості відправлених листів, загальної кількості створених акаунтів в розрізі країни та рейтингів по цим даним.
AS (
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    SUM (account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
    SUM (sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
FROM sum_t
),
runk_t -- вивести рейтінги
AS (
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    total_country_account_cnt,
    total_country_sent_cnt,
    DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
    DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt,
FROM total_t
)
-- Ітоговий запит, що відфільтровує країни по кількості листів та акаунтів
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    total_country_account_cnt,
    total_country_sent_cnt,
    rank_total_country_account_cnt,
    rank_total_country_sent_cnt,
FROM runk_t


WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <=10
