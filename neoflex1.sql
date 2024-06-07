/*1. Выведите в алфавитном порядке фамилию, имя и название отдела для всех сотрудников из Торонто.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME, DEPARTMENT_NAME */
SELECT emps.last_name,
       emps.first_name,
       deps.department_name
  FROM employees emps
 INNER JOIN departments deps USING (department_id)
 INNER JOIN locations   locs ON locs.location_id = deps.location_id
 WHERE locs.city = 'Toronto'
 ORDER BY 1, 2, 3 ASC;

/*Использовал inner join, т.к. нужно выдать всех сотрудников из Торонто, другие строки из отношений employees и departments нас не интересуют.
Использовал using как пример соединения, когда соединительные столбцы в таблицах называются одинаково.
Department_id в таблице employees является внешним ключём к таблице departments. Соединяем через этот столбец.
Далее столбцы в join можно представлять внешними ключами к другим таблицам, чтобы не нагромождать комментарии.
Т.к код помещается на экран (16 дюймов в моем случае), в сортировке использовал номера столбцов. 
Ключевое слово "INNER" добавил для наглядности кода (явно указал соединение).
Сортировку сделал по номерам столбцов для сокращения кода. --В случае, 
если бы у нас были сотрудники с одинаковыми фамилиями и именами, но в разных отделах, 
сортировка по нескольким столбцам была бы наглядной.
Ключевое слово "ASC" добавил для наглядности кода, что сортировка идет в алфавитном порядке.*/

/*2. Выведите адреса всех отделов: почтовый код, улицу, город, штат и страну. Используйте NATURAL JOIN.
Отсортируйте результат по стране и почтовому коду.
Результат: таблица со столбцами POSTAL_CODE, STREET_ADDRESS, CITY, STATE_PROVINCE, COUNTRY_NAME */
SELECT locs.postal_code,
       locs.street_address,
       locs.city,
       locs.state_province,
       cntrs.country_name
  FROM departments deps
NATURAL JOIN locations locs
NATURAL JOIN countries cntrs
 ORDER BY cntrs.country_name,
          locs.postal_code;
          
/*В парах таблиц deprtments - locations и locations - countries есть столбцы, 
по которым можно их соединить через natural join без попадания лишних строк в результат.
Поэтому natural join можно здесь использовать без угрозы некорректности данных в результате.*/

/*3. Выведите ID всех сотрудников, нанятых раньше, чем их менеджеры. Отсортируйте сотрудников по дате найма.
Результат: таблица со столбцами EMPLOYEE_ID, EMPLOYEE_HIRE_DATE, MANAGER_ID, MANAGER_HIRE_DATE.*/
SELECT empls.employee_id,
       empls.hire_date   AS employee_hire_date,
       mngrs.employee_id AS manager_id,
       mngrs.hire_date   AS manager_hire_date
  FROM employees empls
  JOIN employees mngrs ON mngrs.employee_id = empls.manager_id
 WHERE empls.hire_date < mngrs.hire_date
 ORDER BY empls.hire_date;
 
/*Эту задачу можно решить через общее табличное выражение, создав CTE со столбцами "employee_id AS manager_id", 
"hire_date AS manager_hire_date" и совершить с ней inner join к таблице employees.*/

/*4. Выведите фамилию, имя и ID сотрудника вместе с фамилией, именем и ID его менеджера. 
Фамилию и имя каждого человека объедините в отдельный столбец и разделите запятой. 
Для сотрудников, у которых нет менеджера, в качестве ID менеджера проставьте -1.
Отсортируйте сотрудников в алфавитном порядке.
Результат: таблица со столбцами EMPLOYEE, EMPLOYEE_ID, MANAGER, MANAGER_ID.
Подсказка: используйте функции COALESCE, NULLIF.*/
SELECT concat (empls.last_name, ', ', empls.first_name) AS employee,
       empls.employee_id,
       nullif (concat (mngrs.last_name, ', ', mngrs.first_name),
               ', ')                                     AS manager,
       coalesce (mngrs.employee_id, - 1)                AS manager_id
  FROM employees empls
  LEFT JOIN employees mngrs ON mngrs.employee_id = empls.manager_id
 ORDER BY employee;
 
/*Ключевое слово "AS" добавил для наглядности кода (явно указал псевдонимы столбцов).
В зависимости от значения поля manager_id, скорректировал значение поля manager. В задании
этого указано не было, но так, по моему мнению, результат выглядит приятнее
Использовал left join, т.к. могли быть сотрудники со значением null в столбце manager_id,
а вывести нужно было всех сотрудников.*/

/*5. Выведите адреса компании в Великобритании, за исключением тех, которые
находятся в графстве Оксфордшир (STATE_PROVINCE = 'Oxford').
Результат: выборка из таблицы LOCATIONS.
Подсказка: обратите внимание, что поле STATE_PROVINCE не является обязательным к заполнению.*/
SELECT *
  FROM locations
 WHERE (state_province != 'Oxford'
    OR state_province IS NULL)
   AND country_id = 'UK';
   
/*В скобки взято выражение (state_province != 'Oxford' OR state_province IS NULL) .
для правильного порядка выполнения запроса. 
Без скобок БД отработает условие state_province != 'Oxford 
или state_province IS NULL AND country_id = 'UK',
т.е. выдаст нам записи со всех стран без графства Оксфордшир или null строки со страной UK.*/

/*6. Выведите отдел, электронную почту сотрудника и электронную почту всех его коллег. 
Если сотрудник является единственным работником или не привязан к отделу,
оставьте поле с почтами коллег пустым. Отсортируйте отделы и почты в алфавитном порядке.
Результат: таблица со столбцами DEPARTMENT_NAME, EMPLOYEE_EMAIL и COLLEAGUE_EMAIL.
Бонус (+1 балл): Выведете почты коллег одной строкой через точку с запятой.
Подсказка: используйте функции STRING_AGG или ARRAY_TO_STRING.*/
WITH email_data AS (
    SELECT deps.department_name,
           deps.department_id,
           e.email
      FROM employees   e
      LEFT JOIN departments deps ON deps.department_id = e.department_id
), res AS (
    SELECT ed.department_name,
           ed.email,
           string_agg (empls.email, ',') AS col_mail
      FROM email_data ed
      LEFT JOIN employees  empls ON empls.department_id = ed.department_id
     WHERE (empls.email != ed.email
        OR empls.email IS NULL)
     GROUP BY ed.department_name,
              ed.email
    UNION ALL
    SELECT ed.department_name,
           ed.email,
           ''
      FROM email_data ed
     WHERE department_id IN (
        SELECT department_id
          FROM (
            SELECT department_id, COUNT (department_id)
              FROM email_data
             GROUP BY department_id
            HAVING COUNT (department_id) = 1
        )
    )
)
SELECT *
  FROM res
 ORDER BY department_name,
          email;
/*Привел данные из разных источников в единую CTE, после чего обрабатывал и проводил в вид,
требуемый в задании.*/

/*7. Выведите фамилию и имя сотрудника, его текущую зарплату, подоходный налог
и зарплату после вычета налога. Зарплату после вычета налога представьте в виде целого числа.
Отсортируйте сотрудников в алфавитном порядке.
Налоговые ставки для регионов принять:
Европа - 35%
Америка - 30%
Азия - 25%
Средний Восток и Африка - 35%
Регион не определен - 30%
Результат: таблица со столбцами LAST_NAME, FIRST_NAME, SALARY, INCOME_TAX, SALARY_WO_TAX.
Подсказка: Для вычисления налоговых ставок используйте CTE*/
WITH data_tbl AS (
    SELECT emps.last_name,
           emps.first_name,
           emps.salary,
           CASE
               WHEN regs.region_name = 'Europe'
               THEN emps.salary * 0.35
               WHEN regs.region_name = 'Americs'
               THEN emps.salary * 0.30
               WHEN regs.region_name = 'Asia'
               THEN emps.salary * 0.25
               WHEN regs.region_name = 'Middle East and Africa'
               THEN emps.salary * 0.35
               ELSE emps.salary * 0.3
           END    AS income_tax,
           salary AS salary_wo_tax
      FROM employees   emps
      LEFT JOIN departments deps ON emps.department_id = deps.department_id
      LEFT JOIN locations   locs ON locs.location_id = deps.location_id
      LEFT JOIN countries   cntrs ON cntrs.country_id = locs.country_id
      LEFT JOIN regions     regs ON regs.region_id = cntrs.region_id
)
SELECT last_name,
       first_name,
       salary,
       income_tax,
       CAST (salary - income_tax AS INT) AS salary_wo_tax
  FROM data_tbl
 ORDER BY last_name,
          first_name;
          
/*Можно было не join таблицу regions, а лишь взять оттуда region_id и задать значения для отношения countries.region_id.
Это было бы не удобно для будущего рефакторинга кода, но такой вариант возможен, с учетом, что таблица regions на 4 строки.
В столбце income_tax, можно было бы выделить непосредственно ставку, но в условии указан "налог", а не "ставка". Надеюсь, не будет ошибкой :)
Сортировка по фамилии и имени для упорядочивания в алфавитном порядке по имени,
если есть сотрудники с одинаковыми фамилиями.*/

/*8. Выведите информацию о сотрудниках, отсортировав продавцов
(JOBS.JOB_TITLE IN ('Sales Manager', 'Sales Representative')) по размеру комиссии и
ФИО, а остальных - по зарплате и ФИО.
Результат: таблица со столбцами JOB_TITLE, LAST_NAME, FIRST_NAME, SALARY, COMMISSION_PCT.
Подсказка: оператор CASE можно использовать в секции WHERE*/
SELECT jobs.job_title,
       emps.last_name,
       emps.first_name,
       emps.salary,
       emps.commission_pct
  FROM jobs
  JOIN employees emps ON jobs.job_id = emps.job_id
 ORDER BY
    CASE
        WHEN jobs.job_title IN ('Sales Manager', 'Sales Representative')
        THEN emps.commission_pct
        ELSE emps.salary
    END,
    emps.last_name,
    emps.first_name;

/*В поле job_id действует ограничение not null, поэтому можно смело делать inner join
Т.к. в условии не указан порядок сортировки, использую по возрастанию/алфавитному порядку
Не сократил jobs, т.к. сокращать до одного символа, скорее, усложняет чтение кода, 
а убирать 1-2 символа, когда их всего 4, скорее, избыточно. Разве что, в случае минмакса можно использовать*/

/*9. Выведите системную дату, а также первый и последний день текущего месяца. Формат дат YYYY-MM-DD.
Результат: таблица со столбцами TODAY, FIRST_DAY_OF_MON, LAST_DAY_OF_MON.*/
SELECT to_char (current_date, 'YYYY-MM-DD') AS today,
       to_char (date_trunc ('month', current_date),
                'YYYY-MM-DD')                AS first_day_of_mon,
       to_char (date_trunc ('month', current_date) + interval '1 month' - interval '1 day',
                'YYYY-MM-DD')                AS last_day_of_mon;

/*10. Выведите фамилии, имена и количество полных недель работы сотрудников отдела продаж ('Sales').
Отсортируйте результат по убыванию стажа сотрудников.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME, WEEKS_OF_EXPERIENCE.*/
SELECT last_name,
       first_name,
       CAST (floor ((current_date - hire_date) / 7) AS INT) AS weeks_of_experience
  FROM employees emps
  JOIN departments deps ON deps.department_id = emps.department_id
   AND deps.department_name = 'Sales'
 ORDER BY weeks_of_experience DESC;

/*С виду, БД само округляет вниз до целого, на всякий случай употребил floor.
Наконец, пригодилось ключевое слово DESC, что означает сортировка по убыванию/в обратном алфавитном порядке*/

/*11. Выведите фамилию, имя, дату устройства на работу и дату первого пересмотра
зарплаты для каждого сотрудника. Зарплату пересматривают в первый понедельник
после 6 месяцев работы в компании. Дату пересмотра зарплаты покажите в формате
"Monday, the 1st of January, 1900".
Отсортируйте результат по убыванию даты пересмотра.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME, HIRE_DATE, APPRAISAL.*/
  WITH app_tbl AS ( 
    SELECT employee_id,
           hire_date + INTERVAL '6 month'                    AS app_date,
           EXTRACT (dow FROM hire_date + INTERVAL '6 month') AS app_date_dow 
      FROM employees
)
SELECT last_name,
       first_name,
       hire_date, 
    CASE
        WHEN app_date_dow = 2 
        THEN to_char(app_date + INTERVAL '6 days','FMDay, "the "DDth" of "FMMonth, YYYY')
        WHEN app_date_dow = 3 
        THEN to_char(app_date + INTERVAL '5 days','FMDay, "the "DDth" of "FMMonth, YYYY')
        WHEN app_date_dow = 4 
        THEN to_char(app_date + INTERVAL '4 days','FMDay, "the "DDth" of "FMMonth, YYYY')
        WHEN app_date_dow = 5 
        THEN to_char(app_date + INTERVAL '3 days','FMDay, "the "DDth" of "FMMonth, YYYY')
        WHEN app_date_dow = 6 
        THEN to_char(app_date + INTERVAL '2 days','FMDay, "the "DDth" of "FMMonth, YYYY')
        WHEN app_date_dow = 7 
        THEN to_char(app_date + INTERVAL '1 days','FMDay, "the "DDth" of "FMMonth, YYYY')
        ELSE to_char(app_date,'FMDay, "the "DDth" of "FMMonth, YYYY')
    END AS appraisal 
  FROM app_tbl
  JOIN employees emps USING (employee_id)
 ORDER BY hire_date desc;
 
/*Сортировка происходит по дате найма, а не дате повышения, как указано в условии, 
т.к. это справедливо одинаковые (в рамках сортировки) значение в данном случае. Проверено.*/

/*12. 7 декабря у компании день рождения. Сотрудники, отработавшие количество лет,
кратное 5 или 10, получат памятные сувениры. Выведете список, кто и какой сувенир получит.
Отсортируйте список по отделам и сотрудникам.
Результат: таблица со столбцами DEPARTMENT_NAME, LAST_NAME, FIRST_NAME, SOUVENIR (возможные значения: '5Y', '10Y').
Подсказка: для улучшения читаемости используйте CTE*/
WITH data_tbl AS (
    SELECT deps.department_name,
           emps.last_name,
           emps.first_name,
           EXTRACT (YEAR FROM age (DATE '2024-12-07', hire_date)) AS work_exp
      FROM employees   emps
      LEFT JOIN departments deps USING (department_id)
)
SELECT department_name,
       last_name,
       first_name,
       CASE
       WHEN work_exp % 10 = 0 
       THEN '10Y'
       WHEN work_exp % 5 = 0 AND work_exp % 10 != 0 
       THEN '5Y'
       END SOUVENIR
  FROM data_tbl
 ORDER BY 1, 2, 3;

/*Предположил, что речь идет о будущем дне рождении, поэтому 7 декабря 2024 года.
В условии не указано, что нужно вывести только награждающихся сотрудников, поэтому
кто не награждается тоже вывел.*/

/*13. Вы пытаетесь найти контакты коллеги, но помните только, что его имя
начинается с Д, а фамилия заканчивается на "штайн" (stein). Напишите соответствующий запрос.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME, PHONE_NUMBER, EMAIL.*/

SELECT last_name,
       first_name,
       phone_number,
       email
  FROM employees
 WHERE first_name LIKE 'D%'
   AND last_name LIKE '%stein';

/*Важно учитывать регистр при синтаксисе like.*/

/*14. Вы пытаетесь найти контакты коллеги, но помните только, что в его фамилии
дважды и при этом не подряд встречается буква 'g'. Напишите соответствующий запрос.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME, PHONE_NUMBER, EMAIL.
Подсказка: учтите, что буква может оказаться как в начале, так и в середине фамилии.*/
SELECT last_name,
       first_name,
       phone_number,
       email
  FROM employees
 WHERE last_name LIKE 'G%g%'
    OR last_name LIKE '%g%g%';

/*В синтаксисе лайк символ % первым или последним символом предполагает, 
что строка может начинается перед первым точным символом 
или заканчивается сразу за ним, что очень удобно. 'G%g%' является тому примером*/

/*15. Выведите города и международные и региональные телефонные коды сотрудников, работающих в этих городах.
Отсортируйте города в алфавитном порядке. Международным кодом считать цифры до первой точки, региональным - между первой
и второй точками. Например, для номера 650.507.9844 международный код равен 650, региональный - 507.
Результат: таблица со столбцами CITY, INTERNATIONAL_CODE, REGIONAL_CODE.
Подсказка: используйте регулярные выражения.*/
SELECT DISTINCT locs.city,
                split_part (emps.phone_number, '.', 1) AS international_code,
                split_part (emps.phone_number, '.', 2) AS regional_code
  FROM locations locs
  JOIN departments deps USING (location_id)
  JOIN employees   emps USING (department_id)
 ORDER BY locs.city;
/*Distinct для уменьшений строк в результате (дубли не нужны)*/

/*16. Выведите фамилию и имя сотрудника, а также размер его комиссии. Если
сотрудник не получает комиссию, выведите 'NO COMISSION'.
Отсортируйте результат по комиссии в порядке убывания (нулевая комиссия должна
идти последней) и по сотрудникам в алфавитном порядке.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME, COMMISSION_PCT.*/
WITH data_tbl AS (
    SELECT last_name,
           first_name,
           CASE
               WHEN commission_pct IS NULL
               THEN 0
               ELSE commission_pct
           END AS commission_pct
      FROM employees emps
     ORDER BY commission_pct DESC,
              last_name ASC,
              first_name ASC
)
SELECT last_name,
       first_name,
       CASE
           WHEN commission_pct = 0
           THEN 'NO COMISSION'
           ELSE CAST (commission_pct AS varchar)
       END
  FROM data_tbl;

/*Сортировка произошла в CTE. Основной запрос необходимо для изменения значений и типов к соответствующим.
Требуется преобразование данных к одному типу: т.к. комиссия была с плавающей точкой,
а необходимое значение 'NO COMISSION' это строка, нужно изменить тип данных на строковый.*/

/*17. Выведите имя и первую букву фамилии сотрудника, а также его зашифрованную
зарплату в виде строки символов '*', где каждый символ соответствует 1000$.
Отсортируйте результат по убыванию зарплаты. Результат: таблица со столбцами EMPLOYEE, SALARY.*/
SELECT concat (first_name, substr (last_name, 1, 1))       AS employee,
       repeat ('*', CAST (floor (salary / 1000) AS INT))   AS salary
  FROM employees
 ORDER BY 2 DESC;
 
/*Сцепил без пробела имя и первый символ фамилии, ибо прямо не указан формат.
Округлил вниз, т.к. нужные целые тысячи, без остатков.*/

/*18. Выведите список сотрудников, которые работают в организации с года ее открытия.
Результат: таблица со столбцами DEPARTMENT, LAST_NAME, FIRST_NAME.
Подсказка: используйте поле EMPLOYEES.HIRE_DATE.*/
SELECT deps.department_name AS department,
       emps.last_name,
       emps.first_name
  FROM employees   emps
  LEFT JOIN departments deps USING (department_id)
 WHERE EXTRACT (YEAR FROM emps.hire_date) = (
    SELECT EXTRACT (YEAR FROM MIN (hire_date))
      FROM employees
);

/*Агрегатная функция мин/макс возвращает давнейшую/новейшую дату, если применяется на столбец дат.
С учетом подсказки, становится ясно, что первые сотрудники были наняты в фирму год её открытия.
В задании не явно указан 1 столбец (_id или _name). Указал name для наглядности.
Left join применил т.к. вдруг кто-то не относится к отделу, а работает с основания. Надо было проверить.*/

/*19. Выведите в алфавитном порядке сотрудников, которые работают в каком-либо IT-отделе.
Используйте подзапрос с IN.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME.*/
SELECT last_name,
       first_name
  FROM employees
 WHERE department_id IN (
    SELECT department_id
      FROM departments
     WHERE department_name LIKE '%IT%'
)
 ORDER BY last_name,
          first_name;
          
/*20. Найдите всех коллег Дэвида Остина (EMPLOYEE_ID = 105) по отделу с такой же должностью, как у него.
Используйте условие IN для выборки по двум столбцам. Результат: выборка из таблицы EMPLOYEES.
Подсказка: изучите первый пример - https://itecnote.com/tecnote/sql-where-in-clausemultiple-columns/ */
SELECT *
  FROM employees
 WHERE (job_id, department_id) IN (
    SELECT job_id, department_id
      FROM employees
     WHERE employee_id = 105
)
   AND employee_id != 105;
   
/*Ссылка ведет на 404 ошибку. Убрал Дэвида из результата, 
т.к. нужно было найти его коллег, он себе коллегой не является:)*/

/*21. Выведите отделы, в которых никто из сотрудников не получает комиссию.
Используйте подзапрос с NOT IN. Результат: выборка из таблицы DEPARTMENTS.
Подсказка: с NOT IN нужна осторожность из-за особенности обработки NULL в
результате подзапроса - https://langtoday.com/?p=712*/
SELECT *
  FROM departments
 WHERE department_id NOT IN (
    SELECT department_id
      FROM employees
     WHERE commission_pct IS NOT NULL
       AND department_id IS NOT NULL
);

/*22. Выведите отделы, в которых кто-либо из сотрудников получает комиссию.
Используйте подзапрос с WHERE EXISTS. Результат: выборка из таблицы DEPARTMENTS.*/
SELECT *
  FROM departments deps
 WHERE EXISTS (
    SELECT 1
      FROM employees emps
     WHERE emps.department_id = deps.department_id
       AND emps.commission_pct IS NOT NULL
);

/*В данной задаче нужно коррелировать  подзапрос с основным запросом ,т.к. exists 
иначе не выдаст результат, т.к. вернёт TRUE и просто отработает внешний запрос*/

/*23. Выведите в алфавитном порядке сотрудников, у которых нет однофамильцев в
компании. Используйте подзапрос с WHERE NOT EXISTS.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME.*/
SELECT last_name,
       first_name
  FROM employees emps_out
 WHERE NOT EXISTS (
    SELECT 1
      FROM employees emps_in
     WHERE emps_in.last_name = emps_out.last_name       --убеждаемся что это сотрудник с такой же фамилией
       AND emps_in.employee_id <> emps_out.employee_id  --убеждаемся, что это не сам сотруднк
);

/*24. Выведите все адреса компании в европейских странах. Используйте подзапрос с ANY.
Отсортируйте адреса по коду страны, городу и почтовому коду.
Результат: таблица со столбцами COUNTRY_ID, CITY, POSTAL_CODE,STREET_ADDRESS.*/
SELECT locs.country_id,
       locs.city,
       locs.postal_code,
       locs.street_address
  FROM locations locs
 WHERE country_id = ANY (
    SELECT country_id
      FROM countries cnts
      JOIN regions regs USING (region_id)
     WHERE regs.region_name = 'Europe'
)
 ORDER BY 1, 2, 3;
 
/*25. Для каждого отдела выведете сотрудника с самой высокой зарплатой.
Используйте подзапрос с ANY. Отсортируйте результат по названию отдела.
Результат: таблица со столбцами DEPARTMENT_NAME, LAST_NAME, FIRST_NAME,SALARY.*/
SELECT deps.department_name,
       emps.last_name,
       emps.first_name,
       emps.salary
  FROM employees   emps
  LEFT JOIN departments deps USING (department_id)
 WHERE (emps.department_id,
        emps.salary) = ANY (
    SELECT department_id,
           MAX (salary) AS mx_sal
      FROM employees
     GROUP BY department_id
)
 ORDER BY deps.department_name;

/* Департамент со значением null не выдало в результате, 
потому что сравнивая null с null будет null, а не true/false.*/

/*26. Выведите фамилию и имя сотрудника, его текущую зарплату и указатель, 
является ли его зарплата ниже средней, средней или выше средней.
Ориентируйтесь на диапазон заработной платы для определенной должности, 
представленный в таблице JOBS. 
Средняя зарплата - с отклонением не более 20% от среднего арифметического по диапазону. 
Отсортируйте сотрудников в алфавитном порядке.
Результат: таблица со столбцами LAST_NAME, FIRST_NAME, SALARY,
SALARY_SUMMARY (возможные значения: 'BELOW AVG', 'AVG', 'ABOVE AVG').
Подсказка: используйте CTE или подзапрос.*/
WITH salaries_val AS (
    SELECT job_id,
           jobs.min_salary,
           (max_salary + jobs.min_salary) / 2 AS avg_salary,
           jobs.max_salary
      FROM jobs
)
SELECT emps.last_name,
       emps.first_name,
       emps.salary,
       CASE
           WHEN emps.salary BETWEEN salaries_val.min_salary AND salaries_val.avg_salary * 0.8
           THEN 'BELOW AVG'
           WHEN emps.salary BETWEEN salaries_val.avg_salary * 1.2 AND salaries_val.max_salary
           THEN 'ABOVE AVG'
           ELSE 'AVG'
       END AS salary_summary
  FROM employees emps
  JOIN salaries_val USING (job_id)
 ORDER BY 1, 2;
 
 /* Можно и одним запросом обойтись, будет громоздко, но тем не менее*/