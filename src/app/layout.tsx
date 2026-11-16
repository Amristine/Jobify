import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "FresherFlow — Your first opportunity starts here",
  description: "Discover fresher jobs and internships across India. Find your next opportunity with FresherFlow.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
