# System Design Problems

I challenge myself to think in multiple dimensions by building these designs as working projects and mimicking the architecture of well-known systems & This Repo Contains The Built Solution.

A collection of practical System Design interview problems, covering the design of scalable, reliable, and high-performance systems.

## Problems

Each problem is organized into its own directory with a write-up, a design diagram, and a working Python implementation.


| # | Problem | What you design | Key concepts | Solution |
|---|---------|-----------------|--------------|----------|
| 1 | **TinyURL: URL Shortener** | Convert long URLs into short links and redirect users to the original. | Hashing / ID generation, read-heavy caching, redirects | [View](./TinyURL) |
| 2 | **Ticket Booking** | Browse events, select seats, and book tickets without double-booking. | Concurrency control, seat locking, transactions | [View](./Ticket-Booking) |
| 3 | **E-Commerce** | Product search, cart, checkout, orders, and payments at scale. | Catalog search, inventory consistency, payment flow | [View](./E-Commerce) |
| 4 | **OTT Platform** | Video streaming with accounts, subscription plans, search, and playback. | CDN, adaptive streaming, subscriptions, metadata store | [View](./OTT-Platform) |
| 5 | **Hotel Booking** | Search, hotel details, booking, and payment with consistent reservations under load. | Availability search, double-booking prevention, payments | [View](./Hotel-Booking) |
| 6 | **Food Delivery** | Nearby restaurants, search, menu, cart, payment, and order tracking. | Geo-search, order lifecycle, real-time tracking | [View](./Food-Delivery) |
| 7 | **Chat App** | One-to-one messaging with delivery and read receipts, plus media. | WebSockets, message ordering, delivery guarantees | [View](./Chat-APP) |
| 8 | **Search Engine** | Crawl, index, and serve search results with page views. | Inverted index, ranking, crawling, query serving | [View](./Search-Engine) |
| 9 | **Job Scheduler** | Submit, schedule, and execute jobs reliably across workers. | Queues (Kafka), coordinator/worker model, retries | [View](./Job-Scheduler) |
| 10 | **Ride Sharing** | Match riders with nearby drivers, calculate fares, track trips, and handle payment. | Geo-indexing, driver matching, live tracking, fare calculation | [View](./Ride-Sharing) |
| 11 | **Social Media Platform** | Posts, follow graph, likes, and a personalized feed. | Feed generation (push vs pull), fan-out, graph storage | [View](./Social-Media) |
| 12 | **Stock Trading Platform** | Login, watchlist, order placement, portfolio, and payments. | Order matching, real-time prices, consistency, auth | [View](./Stock-Trading) |
| 13 | **Notification System** | Templates, user preferences, and delivery over SMS, email, and push. | Message queues, retries, rate limiting, preference rules | [View](./Notification-System) |
| 14 | **Game Leaderboard** | Upload scores and serve top-K rankings across time windows in real time. | Sorted sets, top-K, time-window aggregation, real-time updates | [View](./Game-Leaderboard) |
| 15 | **Collaborative Text Editor** | Multi-user real-time document editing, Google Docs style. | Operational transform / CRDTs, WebSockets, conflict resolution | [View](./Text-Editor) |

More system design problems will be added as the collection grows.


The [System Design Syllabus](./System%20Design%20Syllabus.pdf) outlines the topics and concepts these problems are built around.

## Goals

- Practice solving real-world system design problems.
- Understand the reasoning behind architectural decisions.
- Explore high-level and low-level design approaches.
- Build a useful reference for system design interviews.
- Improve understanding of scalability, performance, and reliability.

## Author

**Saurab Gyawali**

GitHub: [@soorabcode](https://github.com/soorabcode)


If you find this repository useful, consider giving it a ⭐ on GitHub.

## 👤 Author & Contact
 
**Saurab Gyawali**
 
Click any button below to get in touch or follow my work:
 
<p align="center">
  <a href="mailto:saurabgyawali77@gmail.com">
    <img src="https://img.shields.io/badge/Email-saurabgyawali77%40gmail.com-D14836?style=for-the-badge&logo=gmail&logoColor=white" alt="Email" />
  </a>
</p>
<p align="center">
  <a href="https://github.com/soorabcode">
    <img src="https://img.shields.io/badge/GitHub-soorabcode-181717?style=for-the-badge&logo=github&logoColor=white" alt="GitHub" />
  </a>
  <a href="https://www.linkedin.com/in/saurabgyawalii">
    <img src="https://img.shields.io/badge/LinkedIn-saurabgyawalii-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn" />
  </a>
  <a href="https://medium.com/@saurabgyawali">
    <img src="https://img.shields.io/badge/Medium-@saurabgyawali-000000?style=for-the-badge&logo=medium&logoColor=white" alt="Medium" />
  </a>
</p>
<p align="center">
  <a href="https://www.youtube.com/@saurab_gyawalii">
    <img src="https://img.shields.io/badge/YouTube-@saurab__gyawalii-FF0000?style=for-the-badge&logo=youtube&logoColor=white" alt="YouTube" />
  </a>
  <a href="https://www.instagram.com/saurab_gyawalii/">
    <img src="https://img.shields.io/badge/Instagram-@saurab__gyawalii-E4405F?style=for-the-badge&logo=instagram&logoColor=white" alt="Instagram" />
  </a>
  <a href="https://www.facebook.com/saurabgyawalii/">
    <img src="https://img.shields.io/badge/Facebook-saurabgyawalii-1877F2?style=for-the-badge&logo=facebook&logoColor=white" alt="Facebook" />
  </a>
</p>
