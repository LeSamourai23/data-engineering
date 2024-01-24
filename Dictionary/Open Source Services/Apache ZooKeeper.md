ZooKeeper is a centralized service for maintaining configuration information, naming, providing distributed synchronization, and providing group services. All of these kinds of services are used in some form or another by distributed applications. Each time they are implemented there is a lot of work that goes into fixing the bugs and race conditions that are inevitable. Because of the difficulty of implementing the se kinds of services, applications initially usually skimp on them, which make them brittle in the presence of change and difficult to manage. Even when done correctly, different implementations of these services lead to management complexity when the applications are deployed.

## ChatGPT Explaination
Imagine you have this bustling city with many neighborhoods, each having its own post office (Kafka). Now, in this city, you need someone to manage and coordinate all the post offices to ensure everything runs smoothly, and that's where Zookeeper comes in.

**Zookeeper (The City Planner):**
Zookeeper is like the city planner. Its main job is to keep track of all the post offices (Kafka brokers) in the city and make sure they are working together seamlessly. Here's how:

1. **Coordination and Organization:**
   Just like a city planner organizes infrastructure projects, Zookeeper organizes and coordinates the various Kafka brokers. It keeps a list of which post offices (brokers) are there and helps them communicate effectively.

2. **Leader Election (Post Office Manager):**
   In each post office (Kafka broker), there's a manager (leader) that takes care of things. Zookeeper helps in electing and maintaining this manager. If the manager is on a lunch break or something happens, Zookeeper ensures a new manager is elected without chaos.

3. **Configuration Management (City Blueprint):**
   When the city decides to build a new road or change traffic rules, the city planner updates the city's blueprint. Similarly, Zookeeper manages configurations for Kafka. If there's a change in the number of post offices, partitions, or any other settings, Zookeeper keeps track of it.

4. **Fault Tolerance (City Emergency Plan):**
   Imagine there's a sudden roadblock in the city. The city planner has an emergency plan to manage the situation. Zookeeper does something similar for Kafka. If a post office (broker) faces issues, Zookeeper helps redirect the traffic smoothly to other functioning post offices.

5. **Synchronization (City-wide Clock):**
   In a city, you need a synchronized clock to ensure events happen at the right time. Zookeeper provides a kind of clock for Kafka, helping different post offices (brokers) stay in sync and work together without confusion.

In essence, Zookeeper is the behind-the-scenes city planner that makes sure your Kafka city runs efficiently and stays organized. It's the guardian that helps Kafka maintain order and harmony, ensuring that your messages get where they need to go, no matter how busy the city becomes.

## Basics
* Open Source Apache Project
* Distributed **Key Value Store**
* Maintains **configuration information**
* Stores **ACLs** and **Secrets**
* Enables highly reliable **distributed coordination**
* Provides **distributed synchronization**