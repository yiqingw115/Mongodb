conn = new Mongo();

db = conn.getDB("a1");

//Q1
print("======================Q1========================")
cursor = db.tweets.aggregate([
    {$match:{$and:[{replyto_id:{$exists:false}},{retweet_id:{$exists:false}}]}},
    {$project:{general:"$id"}},
    {$count:"general"}
    ])

// display the result

while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

cursor = db.tweets.aggregate([
    {$match:{replyto_id:{$exists:true}}},
    {$project:{reply:"$id"}},
    {$count:"reply"}
    ])

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

cursor = db.tweets.aggregate([
    {$match:{retweet_id:{$exists:true}}},
    {$project:{retweet:"$id"}},
    {$count:"retweet"}
    ])

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Q2
print("======================Q2========================")
cursor = db.tweets.aggregate([
    {$match:{hash_tags:{$exists:true}}},
    {$match:{$or:[{$and:[{replyto_id:{$exists:false}},{retweet_id:{$exists:false}}]},{replyto_id:{$exists:true}}]}},
    {$unwind:"$hash_tags"},
    {$group:{_id: "$hash_tags.text", count_number: {$sum:1}}},
    {$sort:{count_number:-1}},
    {$limit: 5}
    ])

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Q3
print("======================Q3========================")
//find out the first reply time of each tweet
cursor = db.tweets.aggregate([
    {$project:{replyto_id:1,_id:0,replytime:{$dateFromString:{format:"%Y-%m-%d %H:%M:%S",dateString:"$created_at"}}}},
    {$group:{_id:"$replyto_id",first:{$min:"$replytime"}}},
    {$out:"tweets_q3"},
    ])

//calculate the time to receive a reply and find the longest one
cursor = db.tweets.aggregate([
    {$project:{id:1,replyto_id:1,_id:0,orginaltime:{$dateFromString:{format:"%Y-%m-%d %H:%M:%S",dateString:"$created_at"}}}},
    {$lookup:{from:"tweets_q3",localField:"id",foreignField:"_id",as:"duration"}},
    {$unwind:"$duration"},
    {$project:{id:"$id",duration:{$divide:[{$subtract:["$duration.first","$orginaltime"]},1000]},"_id":0}},
    {$sort:{duration:-1}},
    {$limit:1}
    ])

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Q4
print("======================Q4========================")
//check the retweet_count of each tweet
cursor = db.tweets.aggregate([
    {$match:{$or:[{$and:[{replyto_id:{$exists:false}},{retweet_id:{$exists:false}}]},{replyto_id:{$exists:true}}]}},
    {$project:{id:1,retweet_count:1,_id:0}},
    {$out:"retweet_count"}
    ])


//find the retweets in the database
cursor = db.tweets.aggregate([
    {$project:{retweet_id:1,_id:0}},
    {$out:"db_count"}
    ])
    
//calculate the number of general and reply tweets that the number of retweet_count is greater than db_count
cursor = db.retweet_count.aggregate([
    {$lookup:{from:"db_count",localField:"id",foreignField:"retweet_id",as:"retweets"}},
    {$project:{id:1,retweet_count:1,db_count:{$size:"$retweets"}}},
    {$match:{$expr:{$gt:["$retweet_count","$db_count"]}}},
    {$count:"id"}
    ])

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Q5
print("======================Q5========================")

//find the tweet that have a reply or a retweet and get their parent tweet's id
//the output format is like "_id" : ObjectId("5f682acdafb052103ee4f8e1"),  "MergedArray" : NumberLong(1298449801533632513)
cursor = db.tweets.aggregate([
    {$match:{$or:[{replyto_id:{$exists:true}},{retweet_id:{$exists:true}}]}},
    {$group:{"_id": 0, "replyto":{$push:"$replyto_id"},"retweet":{$push:"$retweet_id"}}},
    {$project:{"MergedArray":["$replyto","$retweet" ] }},
    {$unwind:"$MergedArray"},
    {$unwind:"$MergedArray"},
    {$project:{MergedArray:1, _id :0 }},
    {$out:"tweets_q5"}
    ])

 //Find out the number of tweets that do not have its parent tweet object in the data set   
cursor = db.tweets_q5.aggregate([
    {$lookup:{from:"tweets",localField:"MergedArray",foreignField:"id",as:"whether"}},
    {$match:{whether: []}},
    {$count:"whether"}
    ])

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}


//Q6
print("======================Q6========================")

//find out the tweet which have a reply or a tweet and get their tweet id
cursor = db.tweets.aggregate([
    {$match:{$or:[{replyto_id:{$exists:false}},{retweet_id:{$exists:false}}]}},
    {$group:{"_id": 0, "replyto":{$addToSet:"$replyto_id"},"retweet":{$addToSet:"$retweet_id"}}},
    {$project:{"replyorretweet":{$setUnion:["$replyto","$retweet" ] }}},
    {$unwind:"$replyorretweet"},
    {$project:{replyorretweet:1, _id :0 }},
    {$out:"tweets_q6"}
    
    ])

//Find out the number of general tweets that do not have a reply nor a retweet in the data set.    
cursor = db.tweets.aggregate([
    {$match:{$and:[{replyto_id:{$exists:false}},{retweet_id:{$exists:false}}]}},
    {$lookup:{from:"tweets_q6",localField:"id",foreignField:"replyorretweet",as:"check"}},
    {$match:{check: []}},
    {$count:"id"}
    ])

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}


// drop the newly created collection
db.tweets_q3.drop()
db.retweet_count.drop()
db.db_count.drop()
db.tweets_q5.drop()
db.tweets_q6.drop()

