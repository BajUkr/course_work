--1.1 Design ER-diagram 
-- https://media.geeksforgeeks.org/wp-content/uploads/20240226185635/MusicStreamingER-(1).webp
-- 1.2 Develop OLTP solution
-- Creating the User table
CREATE TABLE "user" (
    userid int PRIMARY KEY,
    username varchar(255),
    email varchar(255),
    password varchar(255)
);

-- Creating the artist table
CREATE TABLE "artist" (
    artistid int PRIMARY KEY,
    name varchar(255),
    genre varchar(255)
);

-- Creating the album table
CREATE TABLE "album" (
    albumid int PRIMARY KEY,
    title varchar(255),
    artistid int,
    genre varchar(255),
    releasedate date,
    FOREIGN KEY (artistid) REFERENCES "artist"(artistid)
);

-- Creating the track table
CREATE TABLE "track" (
    trackid int PRIMARY KEY,
    title varchar(255),
    artistid int,
    albumid int,
	playcount int,
    duration time,
    releasedate date,
    FOREIGN KEY (artistid) REFERENCES "artist"(artistid),
    FOREIGN KEY (albumid) REFERENCES "album"(albumid)
);

-- Creating the playlist table
CREATE TABLE "playlist" (
    playlistid int PRIMARY KEY,
    userid int,
    title varchar(255),
    creationdate date,
    FOREIGN KEY (userid) REFERENCES "user"(userid)
);

-- Creating the like table
CREATE TABLE "like" (
    likeid int PRIMARY KEY,
    userid int,
    trackid int,
    FOREIGN KEY (userid) REFERENCES "user"(userid),
    FOREIGN KEY (trackid) REFERENCES "track"(trackid)
);

-- Creating the premium feature table
CREATE TABLE "premiumfeature" (
    premium_feature_id int PRIMARY KEY,
    name varchar(255)
);

-- Creating the subscription plan table
CREATE TABLE "subscriptionplan" (
    subscription_plan_id int PRIMARY KEY,
    name varchar(255),
    price decimal(10,2),
    description text
);

-- Creating the payment table
CREATE TABLE "payment" (
    payment_id int PRIMARY KEY,
    user_id int,
    amount decimal(10,2),
    date date,
    method varchar(50),
    FOREIGN KEY (user_id) REFERENCES "user"(userid)
);

-- Functions and procedures

-- function to add like
CREATE OR REPLACE FUNCTION add_like(p_user_id INT, p_track_id INT)
RETURNS VOID AS $$
BEGIN
    -- Check if the like already exists
    IF EXISTS (SELECT 1 FROM "like" WHERE UserID = p_user_id AND TrackID = p_track_id) THEN
        RAISE NOTICE 'User already liked this track.';
    ELSE
        -- Insert the like
        INSERT INTO "like" (UserID, TrackID) VALUES (p_user_id, p_track_id);
    END IF;
END;
$$ LANGUAGE plpgsql;


-- function to remove like
CREATE OR REPLACE FUNCTION remove_like(p_user_id INT, p_track_id INT)
RETURNS VOID AS $$
BEGIN
    -- Delete the like
    DELETE FROM "like"
    WHERE UserID = p_user_id AND TrackID = p_track_id;

    IF NOT FOUND THEN
        RAISE NOTICE 'No like was found for this user and track combination.';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to proccess payment

CREATE OR REPLACE FUNCTION process_payment(p_user_id INT, p_amount DECIMAL, p_method VARCHAR, p_plan_id INT)
RETURNS TEXT AS $$
DECLARE
    v_subscription_id INT;
BEGIN
    -- Insert the payment record
    INSERT INTO Payment (User_ID, Amount, Date, Method)
    VALUES (p_user_id, p_amount, CURRENT_DATE, p_method);

    -- Retrieve the latest subscription ID if exists
    SELECT Subscription_Plan_ID INTO v_subscription_id FROM SubscriptionPlan
    WHERE Subscription_Plan_ID = p_plan_id;

    -- Update or create subscription
    IF v_subscription_id IS NOT NULL THEN
        INSERT INTO Subscriptions (User_ID, Plan_ID, Start_Date, End_Date)
        VALUES (p_user_id, p_plan_id, CURRENT_DATE, CURRENT_DATE + INTERVAL '1 year');
        RETURN 'Payment accepted and subscription updated.';
    ELSE
        RETURN 'Payment accepted but the plan was not found.';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- function to create new album
CREATE OR REPLACE FUNCTION create_new_album(p_title VARCHAR, p_artist_id INT, p_genre VARCHAR, p_release_date DATE)
RETURNS VOID AS $$
BEGIN
    INSERT INTO Album (Title, ArtistID, Genre, ReleaseDate)
    VALUES (p_title, p_artist_id, p_genre, p_release_date);
END;
$$ LANGUAGE plpgsql;

-- function to create new artist
CREATE OR REPLACE FUNCTION create_new_artist(p_name VARCHAR, p_genre VARCHAR)
RETURNS VOID AS $$
BEGIN
    INSERT INTO Artist (Name, Genre)
    VALUES (p_name, p_genre);
END;
$$ LANGUAGE plpgsql;

-- PROCEDURE to delete an album
CREATE OR REPLACE PROCEDURE delete_album(p_album_id INT)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM Album WHERE AlbumID = p_album_id;
END;
$$;

-- PROCEDURE to delete an artist
CREATE OR REPLACE PROCEDURE delete_artist(p_artist_id INT)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM Artist WHERE ArtistID = p_artist_id;
END;
$$;

-- FUNCTION to update the users email
CREATE OR REPLACE FUNCTION update_user_email(p_user_id INT, p_new_email VARCHAR)
RETURNS VOID AS $$
BEGIN
    UPDATE "user" SET Email = p_new_email WHERE UserID = p_user_id;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION to update the playlist name
CREATE OR REPLACE FUNCTION update_playlist_title(p_playlist_id INT, p_new_title VARCHAR)
RETURNS VOID AS $$
BEGIN
    UPDATE Playlist SET Title = p_new_title WHERE PlaylistID = p_playlist_id;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION to create new playlist
CREATE OR REPLACE FUNCTION create_new_playlist(p_user_id INT, p_title VARCHAR, p_creation_date DATE)
RETURNS VOID AS $$
BEGIN
    INSERT INTO Playlist (UserID, Title, CreationDate)
    VALUES (p_user_id, p_title, p_creation_date);
END;
$$ LANGUAGE plpgsql;

-- FUNCTION to add track to an album
CREATE OR REPLACE FUNCTION add_track_to_album(p_track_id INT, p_album_id INT)
RETURNS VOID AS $$
BEGIN
    UPDATE Track SET AlbumID = p_album_id WHERE TrackID = p_track_id;
END;
$$ LANGUAGE plpgsql;

-- function to Retrieve sub status
CREATE OR REPLACE FUNCTION check_subscription_status(p_user_id INT)
RETURNS TEXT AS $$
DECLARE
    v_end_date DATE;
BEGIN
    SELECT End_Date INTO v_end_date FROM Subscriptions WHERE User_ID = p_user_id ORDER BY End_Date DESC LIMIT 1;
    IF v_end_date IS NULL THEN
        RETURN 'No active subscription';
    ELSIF v_end_date < CURRENT_DATE THEN
        RETURN 'Subscription expired';
    ELSE
        RETURN 'Subscription active';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ROLES AND PERMISSIONS

-- Drops
DROP ROLE IF EXISTS admin_role;
DROP ROLE IF EXISTS user_role;
DROP ROLE if exists group_admins;
DROP ROLE if exists group_users;


-- Create a role for admin
CREATE ROLE admin_role NOINHERIT LOGIN PASSWORD 'secure_password';

-- Create a role for general users
CREATE ROLE user_role NOINHERIT LOGIN PASSWORD 'user_password';

-- Create a group role for admins
CREATE ROLE group_admins;

-- Create a group role for users
CREATE ROLE group_users;

-- Grant roles to group roles
GRANT group_admins TO admin_role;
GRANT group_users TO user_role;

-- Grant all privileges on tables to admins
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO group_admins;

-- Grant select, insert, update, and delete on specific tables to users
GRANT SELECT, INSERT, UPDATE, DELETE ON Track, Album, Artist TO group_users;

-- Grant execute on specific functions and procedures to users
GRANT EXECUTE ON FUNCTION add_like TO group_users;
GRANT EXECUTE ON FUNCTION remove_like TO group_users;
GRANT EXECUTE ON FUNCTION process_payment TO group_users;

-- Grant execute on all functions and procedures to admins
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO group_admins;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA public TO group_admins;


--1.4 data load
-- Creating temporary tables
CREATE TEMP TABLE temp_users AS SELECT * FROM "user" WHERE 1=0;
CREATE TEMP TABLE temp_artists AS SELECT * FROM Artist WHERE 1=0;
CREATE TEMP TABLE temp_albums AS SELECT * FROM Album WHERE 1=0;
CREATE TEMP TABLE temp_tracks AS SELECT * FROM Track WHERE 1=0;
CREATE TEMP TABLE temp_playlists AS SELECT * FROM Playlist WHERE 1=0;
CREATE TEMP TABLE temp_likes AS SELECT * FROM "like" WHERE 1=0;
CREATE TEMP TABLE temp_premium_features AS SELECT * FROM PremiumFeature WHERE 1=0;
CREATE TEMP TABLE temp_subscription_plans AS SELECT * FROM SubscriptionPlan WHERE 1=0;
CREATE TEMP TABLE temp_payments AS SELECT * FROM Payment WHERE 1=0;

-- Loading data into temporary tables
COPY temp_users FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Users.csv' DELIMITER ',' CSV HEADER;
COPY temp_artists FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Artists.csv' DELIMITER ',' CSV HEADER;
COPY temp_albums FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Albums.csv' DELIMITER ',' CSV HEADER;
COPY temp_tracks FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Tracks.csv' DELIMITER ',' CSV HEADER;
COPY temp_playlists FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Playlists.csv' DELIMITER ',' CSV HEADER;
COPY temp_likes FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Likes.csv' DELIMITER ',' CSV HEADER;
COPY temp_premium_features FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\PremiumFeatures.csv' DELIMITER ',' CSV HEADER;
COPY temp_subscription_plans FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\SubscriptionPlans.csv' DELIMITER ',' CSV HEADER;
COPY temp_payments FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Payments.csv' DELIMITER ',' CSV HEADER;

-- Inserting unique data from temporary tables into main tables
INSERT INTO "user" SELECT * FROM temp_users WHERE NOT EXISTS (SELECT 1 FROM "user" WHERE "user".UserID = temp_users.UserID);
INSERT INTO Artist SELECT * FROM temp_artists WHERE NOT EXISTS (SELECT 1 FROM Artist WHERE Artist.ArtistID = temp_artists.ArtistID);
INSERT INTO Album SELECT * FROM temp_albums WHERE NOT EXISTS (SELECT 1 FROM Album WHERE Album.AlbumID = temp_albums.AlbumID);
INSERT INTO Track SELECT * FROM temp_tracks WHERE NOT EXISTS (SELECT 1 FROM Track WHERE Track.TrackID = temp_tracks.TrackID);
INSERT INTO Playlist SELECT * FROM temp_playlists WHERE NOT EXISTS (SELECT 1 FROM Playlist WHERE Playlist.PlaylistID = temp_playlists.PlaylistID);
INSERT INTO "like" SELECT * FROM temp_likes WHERE NOT EXISTS (SELECT 1 FROM "like" WHERE "like".LikeID = temp_likes.LikeID);
INSERT INTO PremiumFeature SELECT * FROM temp_premium_features WHERE NOT EXISTS (SELECT 1 FROM PremiumFeature WHERE PremiumFeature.Premium_Feature_ID = temp_premium_features.Premium_Feature_ID);
INSERT INTO SubscriptionPlan SELECT * FROM temp_subscription_plans WHERE NOT EXISTS (SELECT 1 FROM SubscriptionPlan WHERE SubscriptionPlan.Subscription_Plan_ID = temp_subscription_plans.Subscription_Plan_ID);
INSERT INTO Payment SELECT * FROM temp_payments WHERE NOT EXISTS (SELECT 1 FROM Payment WHERE Payment.Payment_ID = temp_payments.Payment_ID);

-- Dropping temporary tables
DROP TABLE temp_users;
DROP TABLE temp_artists;
DROP TABLE temp_albums;
DROP TABLE temp_tracks;
DROP TABLE temp_playlists;
DROP TABLE temp_likes;
DROP TABLE temp_premium_features;
DROP TABLE temp_subscription_plans;
DROP TABLE temp_payments;
