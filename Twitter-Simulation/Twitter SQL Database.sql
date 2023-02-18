create database Twitter;
use Twitter;
create table tweet(
    tweet_id int auto_increment primary key,
    user_id int,
    tweet_ts datetime default current_timestamp,
    tweet_text varchar(140)
);

create table follows(
    user_id int,
    follows_id int
);

