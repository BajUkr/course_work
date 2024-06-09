-- Dimension Tables
CREATE TABLE "dim_user" (
    userid int,
    username varchar(255),
    email varchar(255),
    startdate date,
    enddate date,
    iscurrent boolean,
    PRIMARY KEY (userid, startdate, enddate, iscurrent)
);

CREATE TABLE "dim_artist" (
    artistid int PRIMARY KEY,
    name varchar(255),
    genre varchar(255)
);

CREATE TABLE "dim_album" (
    albumid int PRIMARY KEY,
    title varchar(255),
    releasedate date
);

CREATE TABLE "dim_track" (
    trackid int PRIMARY KEY,
    title varchar(255),
	playcount int,
    duration interval
);

CREATE TABLE "dim_time" (
    dateid int PRIMARY KEY,
    date date,
    month int,
    quarter int,
    year int,
	day int
);

-- Fact Tables
-- Adjust Fact_Streaming to include StartDate
CREATE TABLE "fact_streaming" (
    trackid int,
    playcount int,
    listeningtime bigint,
    FOREIGN KEY (trackid) REFERENCES "dim_track" (trackid)
);

-- Adjust Fact_Subscription to include StartDate
CREATE TABLE "fact_subscription" (
    userid int,
    startdate date,
	enddate date,
	iscurrent bool,
    dateid int,
    subscriptionplanid int,
    monthlyfee decimal(10,2),
    FOREIGN KEY (userid, startdate, enddate, iscurrent) REFERENCES "dim_user" (userid, startdate, enddate, iscurrent),
    FOREIGN KEY (dateid) REFERENCES "dim_time" (dateid)
);

CREATE INDEX idx_track ON "fact_streaming"(trackid);
CREATE INDEX idx_subscription_date ON "fact_subscription"(dateid);
CREATE INDEX idx_subscription_user ON "fact_subscription"(userid);

-- QUERIES EXMAPLES

-- Monthly Active Users
SELECT t.year, t.month, COUNT(DISTINCT f.userid) AS MonthlyActiveUsers
FROM fact_streaming f
JOIN dim_time t ON f.dateid = t.dateid
GROUP BY t.year, t.month;

-- Revenue by Subscription Plan and Month
SELECT t.year, t.month, SUM(f.monthlyfee) AS TotalRevenue
FROM fact_subscription f
JOIN dim_time t ON f.dateid = t.dateid
GROUP BY t.year, t.month;

-- Top Tracks by Play Count
SELECT d.trackid, d.title, SUM(f.playcount) AS TotalPlays
FROM fact_streaming f
JOIN dim_track d ON f.trackid = d.trackid
JOIN dim_time t ON f.dateid = t.dateid
WHERE t.date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY d.trackid, d.title
ORDER BY TotalPlays DESC
LIMIT 10;

-- Revenue Generation by Artist
SELECT a.artistid, a.name, SUM(s.monthlyfee) AS TotalRevenue
FROM fact_subscription s
JOIN dim_user u ON s.userid = u.userid AND s.startdate = u.startdate
JOIN fact_streaming fs ON s.userid = fs.userid AND s.startdate = fs.startdate
JOIN dim_track dt ON fs.trackid = dt.trackid
JOIN dim_artist a ON dt.artistid = a.artistid
GROUP BY a.artistid, a.name
ORDER BY TotalRevenue DESC;

-- User Engagement by Month
SELECT t.year, t.month, COUNT(DISTINCT f.userid) AS ActiveUsers, SUM(f.listeningtime) AS TotalListeningTime
FROM fact_streaming f
JOIN dim_time t ON f.dateid = t.dateid
GROUP BY t.year, t.month
ORDER BY t.year, t.month;

-- Subscription Plan Performance
-- Assuming a table "subscription_plan" exists
SELECT sp.subscriptionplanid, sp.name, COUNT(DISTINCT fs.userid) AS SubscriberCount, SUM(fs.monthlyfee) AS TotalRevenue
FROM fact_subscription fs
JOIN subscription_plan sp ON fs.subscriptionplanid = sp.subscriptionplanid
GROUP BY sp.subscriptionplanid, sp.name
ORDER BY TotalRevenue DESC;

-- Growth of User Base Over Time
SELECT t.year, t.month, COUNT(DISTINCT u.userid) AS NewUsers
FROM dim_user u
JOIN dim_time t ON u.startdate = t.date
GROUP BY t.year, t.month
ORDER BY t.year, t.month;

-- Artist Popularity Trend
SELECT a.name, t.year, t.month, SUM(f.playcount) AS TotalPlays
FROM fact_streaming f
JOIN dim_track dt ON f.trackid = dt.trackid
JOIN dim_artist a ON dt.artistid = a.artistid
JOIN dim_time t ON f.dateid = t.dateid
GROUP BY a.name, t.year, t.month
ORDER BY a.name, t.year, t.month;