-- Dimension Tables
CREATE TABLE "dim_user" (
    userid int,
    username varchar(255),
    email varchar(255),
    startdate date,
    enddate date,
    iscurrent boolean,
    PRIMARY KEY (userid, startdate)
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
    userid int,
    startdate date,
    trackid int,
    dateid int,
    playcount int,
    listeningtime bigint,
    FOREIGN KEY (userid, startdate) REFERENCES "dim_user" (userid, startdate),
    FOREIGN KEY (trackid) REFERENCES "dim_track" (trackid),
    FOREIGN KEY (dateid) REFERENCES "dim_time" (dateid)
);

-- Adjust Fact_Subscription to include StartDate
CREATE TABLE "fact_subscription" (
    userid int,
    startdate date,
    dateid int,
    subscriptionplanid int,
    monthlyfee decimal(10,2),
    FOREIGN KEY (userid, startdate) REFERENCES "dim_user" (userid, startdate),
    FOREIGN KEY (dateid) REFERENCES "dim_time" (dateid)
);

CREATE INDEX idx_user ON "fact_streaming"(userid);
CREATE INDEX idx_track ON "fact_streaming"(trackid);
CREATE INDEX idx_date ON "fact_streaming"(dateid);
CREATE INDEX idx_subscription_date ON "fact_subscription"(dateid);
CREATE INDEX idx_subscription_user ON "fact_subscription"(userid);

-- QUERIES EXMAPLES

-- Monthly Active Users
SELECT t.Year, t.Month, COUNT(DISTINCT f.UserID) AS MonthlyActiveUsers
FROM Fact_Streaming f
JOIN Dim_Time t ON f.DateID = t.DateID
GROUP BY t.Year, t.Month;

-- Revenue by Subscription Plan and Month
SELECT t.Year, t.Month, SUM(f.MonthlyFee) AS TotalRevenue
FROM Fact_Subscription f
JOIN Dim_Time t ON f.DateID = t.DateID
GROUP BY t.Year, t.Month;

-- Top Tracks by Play Count
SELECT d.TrackID, d.Title, SUM(f.PlayCount) AS TotalPlays
FROM Fact_Streaming f
JOIN Dim_Track d ON f.TrackID = d.TrackID
JOIN Dim_Time t ON f.DateID = t.DateID
WHERE t.Date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY d.TrackID, d.Title
ORDER BY TotalPlays DESC
LIMIT 10;

-- Revenue Generation by Artist
SELECT a.ArtistID, a.Name, SUM(s.MonthlyFee) AS TotalRevenue
FROM Fact_Subscription s
JOIN Dim_User u ON s.UserID = u.UserID
JOIN Fact_Streaming fs ON s.UserID = fs.UserID
JOIN Dim_Track dt ON fs.TrackID = dt.TrackID
JOIN Dim_Artist a ON dt.ArtistID = a.ArtistID
GROUP BY a.ArtistID, a.Name
ORDER BY TotalRevenue DESC;

-- User Engagement by Month
SELECT t.Year, t.Month, COUNT(DISTINCT f.UserID) AS ActiveUsers, SUM(f.ListeningTime) AS TotalListeningTime
FROM Fact_Streaming f
JOIN Dim_Time t ON f.DateID = t.DateID
GROUP BY t.Year, t.Month
ORDER BY t.Year, t.Month;

-- Subscription Plan Performance
SELECT sp.SubscriptionPlanID, sp.Name, COUNT(DISTINCT fs.UserID) AS SubscriberCount, SUM(fs.MonthlyFee) AS TotalRevenue
FROM Fact_Subscription fs
JOIN SubscriptionPlan sp ON fs.SubscriptionPlanID = sp.SubscriptionPlanID
GROUP BY sp.SubscriptionPlanID, sp.Name
ORDER BY TotalRevenue DESC;

-- Growth of User Base Over Time
SELECT t.Year, t.Month, COUNT(DISTINCT u.UserID) AS NewUsers
FROM Dim_User u
JOIN Dim_Time t ON u.StartDate = t.Date
GROUP BY t.Year, t.Month
ORDER BY t.Year, t.Month;

-- Artist Popularity Trend
SELECT a.Name, t.Year, t.Month, SUM(f.PlayCount) AS TotalPlays
FROM Fact_Streaming f
JOIN Dim_Track dt ON f.TrackID = dt.TrackID
JOIN Dim_Artist a ON dt.ArtistID = a.ArtistID
JOIN Dim_Time t ON f.DateID = t.DateID
GROUP BY a.Name, t.Year, t.Month
ORDER BY a.Name, t.Year, t.Month;

