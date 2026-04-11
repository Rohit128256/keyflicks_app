'use client';
import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { Play, Lock } from 'lucide-react';
import { useAuthStore } from '@/lib/store';
import Link from 'next/link';

export default function Home() {
  const [videoId, setVideoId] = useState('');
  const { isAuthenticated } = useAuthStore();
  const router = useRouter();

  const handleWatch = (e) => {
    e.preventDefault();
    if (videoId.trim()) router.push(`/watch/${videoId.trim()}`);
  };

  return (
    <div className="relative flex flex-col items-center justify-center min-h-[92vh] overflow-hidden text-center px-4">

      {/* ── Background layer ── */}
      <div className="absolute inset-0 -z-20" style={{ background: 'radial-gradient(ellipse 80% 60% at 50% 40%, rgba(255,0,0,0.12) 0%, transparent 70%)' }} />
      <div className="absolute inset-0 -z-20 opacity-[0.035]" style={{
        backgroundImage: 'linear-gradient(rgba(255,255,255,0.4) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.4) 1px, transparent 1px)',
        backgroundSize: '40px 40px'
      }} />

      <div className="flex flex-col items-center -mt-8 w-full">
        {/* ── Top accent bar ── */}
        <div className="flex items-center gap-3 mb-5">
          <span className="h-px w-12 bg-gradient-to-r from-transparent to-accent/60" />
          <span className="text-[10px] font-bold tracking-[0.4em] text-accent/80 uppercase">Premium Access Only</span>
          <span className="h-px w-12 bg-gradient-to-l from-transparent to-accent/60" />
        </div>

        {/* ── Main title ── */}
        <h1
          className="font-black tracking-tight leading-[1.05] mb-2 select-none pb-4"
          style={{ fontSize: 'clamp(2.4rem, 6.5vw, 4.2rem)' }}
        >
          <span className="block text-white drop-shadow-[0_2px_30px_rgba(255,255,255,0.15)]">
            Welcome to
          </span>
          <span
            className="block bg-clip-text text-transparent pb-2"
            style={{ backgroundImage: 'linear-gradient(135deg, #ff3a3a 0%, #ff8080 40%, #ffe0e0 70%, #ff6060 100%)' }}
          >
            KeyFlicks
          </span>
        </h1>

        {/* ── Subtitle badge ── */}
        <div className="flex items-center gap-2 mb-4">
          <span className="h-px flex-1 max-w-[60px] bg-gradient-to-r from-transparent to-white/15" />
          <span className="text-[10px] font-semibold tracking-[0.35em] text-white/40 uppercase">
            A Secret Video Streaming Platform
          </span>
          <span className="h-px flex-1 max-w-[60px] bg-gradient-to-l from-transparent to-white/15" />
        </div>
      </div>

      {/* ── Description ── */}
      <p className="text-white/40 max-w-sm text-sm mb-6 leading-relaxed font-light">
        Watch secret videos using a unique ID or upload new content with our premium streaming service.
      </p>

      {/* ── Action card ── */}
      <div
        className="w-full max-w-md border rounded-2xl p-6 relative"
        style={{
          background: 'rgba(255,255,255,0.03)',
          backdropFilter: 'blur(24px)',
          borderColor: 'rgba(255,255,255,0.08)',
          boxShadow: '0 0 0 1px rgba(255,255,255,0.04) inset, 0 25px 50px rgba(0,0,0,0.5)'
        }}
      >
        {/* inner top glow */}
        <div className="absolute -top-px left-1/2 -translate-x-1/2 w-1/2 h-px bg-gradient-to-r from-transparent via-white/30 to-transparent rounded-full" />

        {isAuthenticated ? (
          <form onSubmit={handleWatch} className="flex flex-col sm:flex-row gap-3">
            <input
              type="text"
              placeholder="Enter Video ID..."
              value={videoId}
              onChange={(e) => setVideoId(e.target.value)}
              className="flex-1 bg-black/40 px-5 py-3 rounded-xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all placeholder:text-white/20 font-mono"
              required
            />
            <button
              type="submit"
              className="flex items-center justify-center gap-2 px-6 py-3 rounded-xl font-semibold text-sm text-white transition-all hover:-translate-y-0.5 active:translate-y-0"
              style={{ background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)', boxShadow: '0 4px 20px rgba(255,0,0,0.4)' }}
            >
              <Play fill="currentColor" size={15} /> Watch
            </button>
          </form>
        ) : (
          <div className="flex flex-col items-center gap-3 py-1">
            <div className="w-9 h-9 rounded-full bg-white/5 border border-white/10 flex items-center justify-center">
              <Lock size={14} className="text-white/50" />
            </div>
            <div>
              <h3 className="text-sm font-semibold text-white mb-1">Log in to Start Watching</h3>
              <p className="text-white/35 text-xs max-w-xs leading-relaxed">
                You need an active account to search for and view secure videos.
              </p>
            </div>
            <Link
              href="/login"
              className="w-full flex items-center justify-center gap-2 py-3 rounded-xl font-semibold text-sm text-white transition-all hover:-translate-y-0.5 active:translate-y-0"
              style={{ background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)', boxShadow: '0 4px 20px rgba(255,0,0,0.35)' }}
            >
              Go to Login →
            </Link>
          </div>
        )}
      </div>

      {/* ── Bottom tagline ── */}
      <p className="mt-8 text-white/20 text-[10px] tracking-[0.2em] uppercase">
        Encrypted · Private · Secure
      </p>
    </div>
  );
}
