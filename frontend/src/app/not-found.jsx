import Link from 'next/link';
import { AlertCircle } from 'lucide-react';

// Global not-found boundary — catches notFound() calls and all 404 routes
export default function NotFound() {
  return (
    <div
      className="min-h-screen flex flex-col items-center justify-center gap-6 px-4"
      style={{ background: '#0f0f0f' }}
    >
      <div className="w-20 h-20 rounded-full flex items-center justify-center"
        style={{ background: 'rgba(255,0,0,0.08)', border: '1px solid rgba(255,0,0,0.15)' }}
      >
        <AlertCircle size={36} className="text-accent" />
      </div>

      <div className="text-center">
        <p className="text-accent/70 text-xs font-bold tracking-[0.4em] uppercase mb-3">404</p>
        <h1 className="text-3xl font-black text-white mb-2 tracking-tight">Page Not Found</h1>
        <p className="text-white/35 text-sm max-w-sm leading-relaxed">
          This page doesn&apos;t exist, or the video you&apos;re looking for has been removed.
        </p>
      </div>

      <Link
        href="/"
        className="px-7 py-3 rounded-xl font-semibold text-sm text-white transition-all hover:-translate-y-0.5 active:translate-y-0"
        style={{ background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)', boxShadow: '0 4px 20px rgba(255,0,0,0.35)' }}
      >
        Back to Home
      </Link>
    </div>
  );
}
