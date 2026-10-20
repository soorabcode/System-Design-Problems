# Hotel Booking Platform (No-Login) — System Design

A system design for a hotel booking platform that works **without user accounts**. A guest can search hotels, view details, book a room, and pay, using only contact details and a booking reference.

## 1. Requirement Analysis

### A. Functional Requirements

1. Search : Find hotels by city/location, dates, guests, filters. Paginated.
2. View : See full details of one hotel: rooms, price, photos, amenities, policies.
3. Book : Reserve a room for given dates. Must never double-book.
4. Payment : Pay for the reservation. Booking is confirmed only after payment succeeds.
   
### B. Non-Functional Requirements
1. Scale : 100M DAU
2. Cap Theorem : Avaialble For Search & Consistent For Book 

## 2. Identifying Entities

1. Users
2. Hotels
   

## 3. API Design

### A. Search

```http
GET /v1/search?query={search}-> List<hotelID> :pagenation 
```

### B. View

```http
GET /v1/hotels/{hotelId} -> Hotels Details
```

### C. Book

```http
POST /v1/book{book body : hotelID}-> bookID
```

### D. Payment

```http
GET /v1/payment={bookID} -> Payment Confirmation
```

## 4. High-Level Design

**Flow:** `user -> API gateway -> services (search, view, book, payment) -> databases`


## 5. Low-Level Design
