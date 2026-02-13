--#########################################################################
--##                                                                     ##
--## CE CODE PROPOSE DES R2PONSES AUX QUESTIONS DANS LE DOCUMENT         ##
--## "Énoncé_projet_NoSQL". IL NE FAUT PAS L'EX2CUTER INT2GRALEMENT,     ##
--##  CA VA FAIRE ERRERU VOUS POUVEZ EX2CUTER LA PARTIE SQL LIGNE PAR    ##
--##             LIGNE POUR REPONDRE LA PARTIE SQL.                      ##
--##                                                                     ##
--## IL FAUT LE TERMINAL CMD POUR EXECUTER LA PARTIE 4 JUSQU'A LA FIN    ##
--##                                                                     ##
--#########################################################################


SELECT * FROM departments ORDER BY dept_no LIMIT 20;

SELECT * FROM employees ORDER BY emp_no LIMIT 20;

SELECT * FROM dept_emp ORDER BY emp_no, from_date LIMIT 20;

SELECT * FROM dept_manager ORDER BY dept_no, from_date LIMIT 20;

SELECT * FROM titles ORDER BY emp_no, from_date LIMIT 20;

SELECT * FROM salaries ORDER BY emp_no, from_date LIMIT 20;



-- Vérifier rapidement les volumes
SELECT 'departments'  AS table, COUNT(*) AS nb_lignes FROM departments
UNION ALL SELECT 'employees',     COUNT(*) FROM employees
UNION ALL SELECT 'dept_emp',      COUNT(*) FROM dept_emp
UNION ALL SELECT 'dept_manager',  COUNT(*) FROM dept_manager
UNION ALL SELECT 'titles',        COUNT(*) FROM titles
UNION ALL SELECT 'salaries',      COUNT(*) FROM salaries;



-- Département actuel (affectations en cours)
SELECT * 
FROM dept_emp
WHERE to_date = DATE '9999-01-01'
ORDER BY emp_no
LIMIT 20;

-- Titres actuels
SELECT *
FROM titles
WHERE to_date = DATE '9999-01-01'
ORDER BY emp_no
LIMIT 20;

-- Salaires actuels (selon le dataset, souvent pareil)
SELECT *
FROM salaries
WHERE to_date = DATE '9999-01-01'
ORDER BY emp_no
LIMIT 20;


-- Explorer la structure (colonnes)
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('employees','departments','dept_emp','dept_manager','titles','salaries')
ORDER BY table_name, ordinal_position;

-- 2.1 — Requête SQL de jointure employees + salaries + titles
SELECT
  employe.emp_no,
  employe.first_name,
  employe.last_name,
  salaire.salary,
  salaire.from_date   AS salaire_date_debut,
  salaire.to_date     AS salaire_date_fin,
  titre.title,
  titre.from_date     AS titre_date_debut,
  titre.to_date       AS titre_date_fin
FROM employees AS employe
JOIN salaries  AS salaire ON salaire.emp_no = employe.emp_no
JOIN titles    AS titre   ON titre.emp_no   = employe.emp_no;




-- 2.2 temps d’exécution
Total rows: 4638507
16 secs 967 msec


-- 2.3 — Une vue matérialisée sur cette jointure
CREATE MATERIALIZED VIEW vue_mat_employes_salaires_titres AS
SELECT
  employe.emp_no,
  employe.first_name,
  employe.last_name,
  salaire.salary,
  salaire.from_date   AS salaire_date_debut,
  salaire.to_date     AS salaire_date_fin,
  titre.title,
  titre.from_date     AS titre_date_debut,
  titre.to_date       AS titre_date_fin
FROM employees AS employe
JOIN salaries  AS salaire ON salaire.emp_no = employe.emp_no
JOIN titles    AS titre   ON titre.emp_no   = employe.emp_no;



-- 2.4 — Temps d’accès à la vue matérialisée
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM vue_mat_employes_salaires_titres;



EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM employees AS employe
JOIN salaries  AS salaire ON salaire.emp_no = employe.emp_no
JOIN titles    AS titre   ON titre.emp_no   = employe.emp_no;





-- 3 Export de données PostgreSQL vers fichiers JSON
-- employees
SELECT row_to_json(ligne) AS document_json
FROM (SELECT * FROM employees ORDER BY emp_no) AS ligne;

-- departments
SELECT row_to_json(ligne) AS document_json
FROM (SELECT * FROM departments ORDER BY dept_no) AS ligne;

-- dept_emp
SELECT row_to_json(ligne) AS document_json
FROM (SELECT * FROM dept_emp ORDER BY emp_no, dept_no, from_date) AS ligne;

-- dept_manager
SELECT row_to_json(ligne) AS document_json
FROM (SELECT * FROM dept_manager ORDER BY dept_no, emp_no, from_date) AS ligne;

-- titles
SELECT row_to_json(ligne) AS document_json
FROM (SELECT * FROM titles ORDER BY emp_no, from_date, title) AS ligne;

-- salaries
SELECT row_to_json(ligne) AS document_json
FROM (SELECT * FROM salaries ORDER BY emp_no, from_date) AS ligne;


--3.2 — Agréger en “tableau JSON” avec json_agg
-- employees
SELECT json_agg(row_to_json(ligne)) AS tableau_json
FROM (SELECT * FROM employees ORDER BY emp_no) AS ligne;

-- departments
SELECT json_agg(row_to_json(ligne)) AS tableau_json
FROM (SELECT * FROM departments ORDER BY dept_no) AS ligne;

-- dept_emp
SELECT json_agg(row_to_json(ligne)) AS tableau_json
FROM (SELECT * FROM dept_emp ORDER BY emp_no, dept_no, from_date) AS ligne;

-- dept_manager
SELECT json_agg(row_to_json(ligne)) AS tableau_json
FROM (SELECT * FROM dept_manager ORDER BY dept_no, emp_no, from_date) AS ligne;

-- titles
SELECT json_agg(row_to_json(ligne)) AS tableau_json
FROM (SELECT * FROM titles ORDER BY emp_no, from_date, title) AS ligne;

-- salaries
SELECT json_agg(row_to_json(ligne)) AS tableau_json
FROM (SELECT * FROM salaries ORDER BY emp_no, from_date) AS ligne;



-- 3.3 Export en JSON
-- Je cré d'abord un dossier pg_exports dans C: puis 

COPY (
  SELECT json_agg(row_to_json(ligne))
  FROM (SELECT * FROM employees ORDER BY emp_no) AS ligne
) TO 'C:/pg_exports/employees.json';

COPY (
  SELECT json_agg(row_to_json(ligne))
  FROM (SELECT * FROM departments ORDER BY dept_no) AS ligne
) TO 'C:/pg_exports/departments.json';

COPY (
  SELECT json_agg(row_to_json(ligne))
  FROM (SELECT * FROM dept_emp ORDER BY emp_no, dept_no, from_date) AS ligne
) TO 'C:/pg_exports/dept_emp.json';

COPY (
  SELECT json_agg(row_to_json(ligne))
  FROM (SELECT * FROM dept_manager ORDER BY dept_no, emp_no, from_date) AS ligne
) TO 'C:/pg_exports/dept_manager.json';

COPY (
  SELECT json_agg(row_to_json(ligne))
  FROM (SELECT * FROM titles ORDER BY emp_no, from_date, title) AS ligne
) TO 'C:/pg_exports/titles.json';

COPY (
  SELECT json_agg(row_to_json(ligne))
  FROM (SELECT * FROM salaries ORDER BY emp_no, from_date) AS ligne
) TO 'C:/pg_exports/salaries.json';



-- 4) Import dans MongoDB (base employees)

-- DANS LE CMD
-- D'abord :
mongod

--4.1 Commandes mongoimport
mongoimport --db employees --collection employees     --file "C:\pg_exports\employees.json"     --jsonArray --drop
mongoimport --db employees --collection departments   --file "C:\pg_exports\departments.json"   --jsonArray --drop
mongoimport --db employees --collection dept_emp      --file "C:\pg_exports\dept_emp.json"      --jsonArray --drop
mongoimport --db employees --collection dept_manager  --file "C:\pg_exports\dept_manager.json"  --jsonArray --drop
mongoimport --db employees --collection titles        --file "C:\pg_exports\titles.json"        --jsonArray --drop
mongoimport --db employees --collection salaries      --file "C:\pg_exports\salaries.json"      --jsonArray --drop


--Puis je lance :
mongosh

use employees
show collections

db.employees.countDocuments()
db.departments.countDocuments()
db.dept_emp.countDocuments()
db.dept_manager.countDocuments()
db.titles.countDocuments()
db.salaries.countDocuments()

db.employees.findOne()


-- 5.0 — Créer des index pour accélérer les $lookup
db.titles.createIndex({ emp_no: 1 })
db.salaries.createIndex({ emp_no: 1 })


-- 5.1 — Jointure employees ↔ titles (1 $lookup)
-- sur 1 employé pour voir
db.employees.aggregate([ { $match: { emp_no: 10001 } }, { $lookup: {from: "titles", localField: "emp_no", foreignField: "emp_no", as: "titles" }}]).toArray()

-- global
db.employees.aggregate([{$lookup: {from: "titles", localField: "emp_no", foreignField: "emp_no", as: "titles"}}]).toArray()

--5.2 — Pipeline à 2 étapes : jointure employees ↔ titles ↔ salaries
const pipeline_jointure_3 = [{$lookup: {from: "titles", localField: "emp_no", foreignField: "emp_no", as: "titles"}},
  {$lookup: {from: "salaries", localField: "emp_no", foreignField: "emp_no", as: "salaries"}}]


-- 5.3 — Mesurer le temps d’exécution de la jointure (MongoDB)
db.employees .explain("executionStats") .aggregate(pipeline_jointure_3, { allowDiskUse: true })


--5.4 — Dénormalisation + $project (supprimer doublons emp_no et _id)
let pipeline_denormalisation = [
  ...pipeline_jointure_3, {$project: { _id: "$emp_no", birth_date: 1, first_name: 1, last_name: 1,
      gender: 1, hire_date: 1,
      titles: {$map: {input: "$titles", as: "t", in: {title: "$$t.title", from_date: "$$t.from_date",
            to_date: "$$t.to_date"}}},
      salaries: {$map: {input: "$salaries", as: "s", in: {salary: "$$s.salary",
            from_date: "$$s.from_date", to_date: "$$s.to_date"}}}}}];

-- J'test sur 1 employé pour vérifier
db.employees.aggregate([{ $match: { emp_no: 10001 } }, ...pipeline_denormalisation], { allowDiskUse: true }).toArray();



--5.5 — Sauvegarder le résultat dans une nouvelle collection ($merge ou $out)
console.time("creation_employees_denormalises");

db.employees.aggregate([
    ...pipeline_denormalisation,
    {$merge: {into: "employees_denormalises", on: "_id", whenMatched: "replace", whenNotMatched: "insert"}}],
  { allowDiskUse: true }).toArray();

console.timeEnd("creation_employees_denormalises");



--5.6 Mesurer le délai d’accès à toutes les infos (après dénormalisation)
db.employees_denormalises.explain("executionStats").find({ _id: 10001 }).limit(1);