# FresherFlow

A fresher-first job discovery frontend built with Next.js, TypeScript, Tailwind CSS and Lucide icons.

## Current status

The repository currently contains the **frontend prototype**. It uses illustrative local mock data so the experience can be designed and tested before connecting storage, ingestion sources or AI classification.

> Demo listings are illustrative and are not live vacancies.

## Features

- Responsive landing page and job discovery interface
- Search by job title, company, role, city and skills
- Filters for city, job type and role category
- Job cards with company, location, compensation and skills
- Save/bookmark interaction on job cards
- Empty state and reset filters
- About and how-it-works sections

## Run locally

Requirements: Node.js 18.18+ (Node.js 20 LTS recommended).

```bash
npm install
npm run dev
```

Open http://localhost:3000.

## Tech stack

- Next.js App Router
- React + TypeScript
- Tailwind CSS v4
- Lucide React

## Roadmap

1. Finish frontend UX and job detail interactions.
2. Connect Supabase for persistent listings.
3. Add permitted public-source ingestion and deduplication.
4. Add Gemini classification with structured outputs.
5. Add scheduled refresh, evaluation and cost reporting.

## Data integrity

Do not present sample records as real job openings. When live ingestion is added, each listing should retain its source, original application URL, collection timestamp and verification status.
