USE game_analysis;

-- CREATE TABLE level_details (
-- 	P_ID INT,
-- 	Dev_ID VARCHAR(30),
-- 	TimeStamp VARCHAR(30),
-- 	Stages_crossed INT,
-- 	Level INT,
-- 	Difficulty VARCHAR(30),
-- 	Kill_Count INT,
-- 	Headshots_Count INT,
-- 	Score INT,
-- 	Lives_Earned INT);
	
-- CREATE TABLE player_details (
-- 	P_ID INT,
-- 	PName VARCHAR(30),
-- 	L1_Status INT,
-- 	L2_Status INT,
-- 	L1_Code VARCHAR(30),
-- 	L2_Code VARCHAR(30));
	
-- alter table player_details modify L1_Status varchar(30);
-- alter table player_details modify L2_Status varchar(30);
-- alter table player_details modify P_ID int primary key;

-- alter table level_details change timestamp start_datetime datetime;
-- alter table level_details modify Dev_Id varchar(10);
-- alter table level_details modify Difficulty varchar(15);
-- alter table level_details add primary key(P_ID,Dev_id,start_datetime);

--Q1
SELECT ld.P_ID, ld.Dev_ID, pd.PName, ld.Difficulty 
FROM player_details as pd JOIN level_details as ld 
ON ld.P_ID=pd.P_ID 
WHERE ld.level=0;
--Q2
SELECT pl.L1_code, AVG(pl.Kill_Count) as Avg_Kill_Count 
FROM ( SELECT pd.L1_Code,ld.kill_count 
	FROM player_details as pd JOIN level_details as ld 
	ON ld.P_ID=pd.P_ID 
	WHERE ld.Lives_Earned=2 AND ld.Stages_crossed>=3) as pl 
	GROUP BY L1_Code;
--Q3
SELECT SUM(lev.Stages_crossed) as total_stages_crossed, lev.Difficulty 
FROM ( SELECT * FROM level_details  
	WHERE Level=2 AND  Dev_ID 
	LIKE 'zm____') as lev 
	GROUP BY Difficulty 
	ORDER BY total_stages_crossed DESC;
--Q4
SELECT P_ID, COUNT(DISTINCT DATE(start_datetime)) as unique_dates 
FROM level_details 
GROUP BY P_ID;
--Q5
SELECT P_ID, Level, SUM(Kill_Count) as Sum_kills 
FROM level_details 
WHERE Difficulty='Medium' AND Kill_Count>(SELECT AVG(Kill_Count) as avg_kill  
FROM level_details) 
GROUP BY P_ID, Level;
--Q6
SELECT ld.Level, SUM(ld.Lives_Earned) as sum_lives, pd.L1_Code, pd.L2_Code 
FROM level_details as ld INNER JOIN player_details as pd 
ON ld.P_ID=pd.P_ID WHERE ld.level>0 
GROUP BY ld.Level,pd.L1_Code, pd.L2_Code 
ORDER BY ld.Level ASC;
--Q7
SELECT ld.Difficulty, ld.P_ID, ld.Score 
FROM (SELECT *, ROW_NUMBER() OVER ( partition by P_ID order by Score desc) AS ranks 
	FROM level_details) 
	AS ld WHERE ld.ranks<=3;
--Q8
SELECT ld.Dev_ID, ld.start_datetime 
FROM (SELECT Dev_ID, start_datetime, 
	ROW_NUMBER() OVER(partition by Dev_ID order by start_datetime asc) AS times 
	FROM level_details) AS ld 
	WHERE ld.times<=1;
--Q9
SELECT ld.Dev_ID, ld.difficulty, ld.ranks 
FROM (SELECT Dev_ID, difficulty, RANK() OVER w AS ranks ,ROW_NUMBER() OVER w AS d_rank 
	FROM level_details WINDOW w AS (partition by difficulty order by Score desc) ) AS ld 
	WHERE ld.d_rank<=5;
--Q10
SELECT ld.P_ID, ld.Dev_ID, ld.start_datetime 
FROM (SELECT P_ID, Dev_ID, start_datetime, 
	ROW_NUMBER() OVER(partition by P_ID order by start_datetime asc) AS times 
	FROM level_details) AS ld 
	WHERE ld.times<=1;
--Q11
 --a
WITH data AS 
(SELECT P_ID, DATE(start_datetime) as day, COUNT(*) AS logs 
	FROM level_details 
	GROUP BY day, P_ID) 
SELECT P_ID, day, SUM(logs) OVER(partition by P_ID order by day) AS cumulative_sum 
FROM data;
 --b
WITH data AS 
(SELECT P_ID, DATE(start_datetime) as day, COUNT(*) AS logs 
	FROM level_details 
	GROUP BY day, P_ID) 
SELECT P_ID, day, cumulative_sum2 
FROM (SELECT P_ID, day, @games:=IF(@id=P_ID, @games+logs, logs) as cumulative_sum2, @id:=P_ID 
	FROM data 
	ORDER BY P_ID, day) AS ld;
--Q12
SELECT P_ID, start_datetime, cumulative_sum 
FROM (SELECT P_ID, start_datetime, 
	SUM(Stages_crossed) OVER(partition by P_ID order by start_datetime) AS cumulative_sum, 
	ROW_NUMBER() OVER ( partition by P_ID order by  start_datetime desc ) AS ranks 
	FROM level_details 
	ORDER BY P_ID, start_datetime) AS ld 
	WHERE ld.ranks>1;
--Q13
WITH data AS 
(SELECT Dev_ID, P_ID, Score, ROW_NUMBER() OVER(partition by Dev_ID order by score desc) AS ranks 
	FROM level_details 
	ORDER BY Dev_ID, Score) 
	SELECT Dev_ID, P_ID, Score 
	FROM data 
	WHERE ranks<=3;
--Q14
WITH sumid AS (SELECT *, AVG(score_sum) OVER() AS average FROM (SELECT P_ID, SUM(score) AS score_sum FROM level_details GROUP BY P_ID) AS ld) SELECT sd.P_ID, MAX(ld.score) AS scored FROM sumid as sd INNER JOIN level_details as ld ON sd.P_ID=ld.P_ID WHERE ld.score>(sd.average*0.5) GROUP BY P_ID;
--Q15
delimiter //
CREATE PROCEDURE dev_headshots (IN n INT)
BEGIN
SELECT Dev_ID, Headshots_Count, Difficulty, ROW_NUMBER() OVER(partition by Dev_ID order by Headshots_Count) AS ran FROM (SELECT *, ROW_NUMBER() OVER(partition by Dev_ID order by Headshots_Count desc) AS ord FROM level_details) AS ld WHERE ord<=n;
END//
delimiter ;
CALL dev_headshots(5);

