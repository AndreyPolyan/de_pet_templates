// Connect to the database
db = db.getSiblingDB("mongo_orders");

// Create 'Users' collection
db.createCollection("users", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "login", "registration_ts", "update_ts"],
      properties: {
        name: { bsonType: "string", description: "must be a string and is required" },
        login: { bsonType: "string", description: "must be a string and is required" },
        registration_ts: { bsonType: "date", description: "must be a date and is required" },
        update_ts: { bsonType: "date", description: "must be a date and is required" },
      },
    },
  },
});