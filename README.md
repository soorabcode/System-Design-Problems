# System Design Problems

I challenge myself to think in multiple dimensions by building these designs as working projects and mimicking the architecture of well-known systems & This Repo Contains The Built Solution.

A collection of practical System Design interview problems, covering the design of scalable, reliable, and high-performance systems.

## Problems

Each problem is organized into its own directory with a write-up, a design diagram, and a working Python implementation.

 
| # | Problem | Description | Scale | CAP Preference | Solution |
|---|---------|-------------|-------|----------------|----------|
| 1 | **TinyURL: URL Shortener** | Convert long URLs into unique 6-character short links with a 3-day expiry and fast redirects. | 1M MAU | High availability | [View](./TinyURL) |
| 2 | **Ticket Booking** | Browse events, view listings, and book seats without double-booking. | 10M DAU | High availability | [View](./Ticket-Booking) |
| 3 | **E-Commerce** | Login, product search and view, cart, checkout, payment, and order tracking. | 10M DAU, 10 orders/sec | High availability | [View](./E-Commerce) |
| 4 | **OTT Platform** | Netflix-style streaming with accounts, subscription plans, search, and playback (StreamGate demo). | 100M MAU, 10K videos | AP | [View](./OTT-Platform) |
| 5 | **Hotel Booking** | Account-free hotel search, details, booking, and payment that never double-books a room. | 100M DAU | Available for search, consistent for booking | [View](./Hotel-Booking) |
| 6 | **Food Delivery** | Nearby restaurants, search, menus, cart, payment, and live order tracking with agent assignment. | 100M MAU | Available for browsing, consistent for orders and payment | [View](./Food-Delivery) |
| 7 | **Search Engine** | Google-style web search with crawling, indexing, ranking, and search and view APIs. | ~10M sites | AP | [View](./Search-Engine) |
| 8 | **Chat App** | WhatsApp-style 1:1 messaging with text, audio and video, and delivery and read receipts. | 10M users, 1000M+ messages | AP | [View](./Chat-APP) |
| 9 | **Job Scheduler** | Distributed scheduler to create, monitor, and cancel jobs with at-least-once execution. | 10K jobs/sec | AP | [View](./Job-Scheduler) |
| 10 | **Text Editor** | Google Docs-style editor with file creation, upload, versioning, and download. | 10M+ users | AP | [View](./Text-Editor) |
| 11 | **Ride Sharing** | Uber-style fare estimates, ride categories, driver matching, live tracking, and payments. | 10M riders, 20K drivers | Riders AP, drivers CP | [View](./Ride-Sharing) |
| 12 | **Stock Trading** | Zerodha-style live prices, buy orders, real-time watchlist, and portfolio dashboard. | 10M+ users | CP | [View](./Stock-Trading) |
| 13 | **Social Media** | Instagram-style image posts, follow and unfollow, personalised feed, timeline, and likes. | 10M DAU | AP | [View](./Social-Media) |
| 14 | **Game Leaderboard** | Real-time score upload, Top-K rankings, and hourly to all-time windows. | 1M requests/sec | AP | [View](./Game-LeaderBoard) |
| 15 | **Notification System** | Multi-channel SMS, email, and push with dynamic templates, user preferences, and delivery reporting. | 1M notifications | AP | [View](./Notification-System) |
| 16 | **Login System** | Login activity platform with multi-source ingestion, Kafka and Flink streaming, validation, and Elasticsearch search. | 10M+ users and events | AP | [View](./Login-System) |
| 17 | **Payment Gateway** | Payment intents, short-lived secure card sessions, checkout, and status tracking. | 10K TPS | CP | [View](./Payment-Gateway) |
| 18 | **Cloud Storage** | Dropbox-style upload of files and folders, multi-device sync, link sharing, and deletion. | Millions of users | AP | [View](./Cloud-Storage) |
| 19 | **Ed-Tech Platform** | Udemy-style courses with enrollment, assessments, reviews, instructor onboarding, and course creation. | 100M+ users, 10K+ courses | AP | [View](./ed-Tech) |
| 20 | **Email Delivery** | Gmail-style registration, send with `to`/`cc`/`bcc` and attachments, dashboard, and mailbox search. | 100M users, 1B emails | AP | [View](./Email-Delivery) |
| 21 | **Rate Limiter** | Per-user, per-IP, per-key, and per-endpoint limits with burst handling and HTTP 429 responses. | 10K requests/sec | AP | [View](./Rate-Limiter) |
| 22 | **Dating App** | Swipe-based app with no login (device ID is the identity): profiles, preferences, recommendations, left/right swipes, WebSocket chat with matches, and image upload. | Not specified | AP, instant swipes served from memory | [View](./Dating-APP) |

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
