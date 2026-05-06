'use client';
import { useEffect, useRef, use } from 'react';
import { api } from '@/lib/api';
import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { User, Play, Video, Loader2, Copy, FileVideo } from 'lucide-react';
import Link from 'next/link';
import { motion, AnimatePresence } from 'framer-motion';
import { formatDistanceToNow } from 'date-fns';
import toast from 'react-hot-toast';

export default function PublicProfilePage({ params }) {
  // Fix for Next.js 15+ where params is a promise
  const unwrappedParams = use(params);
  const username = unwrappedParams.username;
  const observerTarget = useRef(null);

  // Phase 1: Fetch target user details
  const { data: profile, isLoading: isLoadingProfile, error: profileError } = useQuery({
    queryKey: ['profile', username],
    queryFn: async () => {
      const res = await api.get(`/profile/${username}`);
      return res.data;
    },
    retry: 1
  });

  const targetUserId = profile?.userid;

  // Phase 2: Fetch target user's paginated videos
  const {
    data,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    status
  } = useInfiniteQuery({
    queryKey: ['user-videos', targetUserId],
    queryFn: async ({ pageParam }) => {
      const p = new URLSearchParams();
      p.append('userID', targetUserId);
      if (pageParam?.cursor_time) p.append('cursor_time', pageParam.cursor_time);
      if (pageParam?.cursor_id) p.append('cursor_id', pageParam.cursor_id);
      
      const res = await api.get(`/get-videos?${p.toString()}`);
      return res.data;
    },
    getNextPageParam: (lastPage) => {
      if (lastPage.has_more) {
        return {
          cursor_time: lastPage.next_cursor_time,
          cursor_id: lastPage.next_cursor_id
        };
      }
      return undefined;
    },
    enabled: !!targetUserId,
  });

  // Infinite Scroll Observer
  useEffect(() => {
    const observer = new IntersectionObserver(
      entries => {
        if (entries[0].isIntersecting && hasNextPage && !isFetchingNextPage) {
          fetchNextPage();
        }
      },
      { threshold: 1.0 }
    );
    
    if (observerTarget.current) {
      observer.observe(observerTarget.current);
    }
    
    return () => observer.disconnect();
  }, [hasNextPage, isFetchingNextPage, fetchNextPage]);

  if (isLoadingProfile) {
     return (
        <div className="flex h-[80vh] items-center justify-center">
           <div className="flex items-center gap-3 text-white/50">
             <Loader2 className="animate-spin" size={24} /> Locating Creator...
           </div>
        </div>
     );
  }

  if (profileError || !profile) {
    return (
      <div className="flex h-[80vh] items-center justify-center">
         <div className="text-center">
            <User size={64} className="text-white/10 mx-auto mb-4" />
            <h1 className="text-2xl font-bold text-white mb-2">Creator Not Found</h1>
            <p className="text-white/40">The user &quot;{username}&quot; does not exist or has locked their profile.</p>
            <Link href="/" className="inline-block mt-6 px-6 py-2 bg-white/10 hover:bg-white/20 rounded-xl transition-colors">Return Home</Link>
         </div>
      </div>
    );
  }

  return (
    <div className="w-full flex flex-col gap-6 w-full max-w-6xl mx-auto py-4">
      {/* ── Top Profile Summary ── */}
      <div
        className="w-full rounded-3xl relative overflow-hidden z-10"
        style={{
          background: 'linear-gradient(135deg, rgba(20,20,20,0.95) 0%, rgba(12,12,12,0.98) 100%)',
          border: '1px solid rgba(255,255,255,0.07)',
          boxShadow: '0 0 0 1px rgba(255,255,255,0.04) inset, 0 20px 60px rgba(0,0,0,0.5)'
        }}
      >
        {/* Ambient red glow top-left */}
        <div className="absolute -top-10 -left-10 w-48 h-48 rounded-full bg-accent/10 blur-3xl pointer-events-none" />
        {/* Subtle diagonal shine */}
        <div className="absolute inset-0 pointer-events-none" style={{ background: 'linear-gradient(115deg, rgba(255,255,255,0.03) 0%, transparent 50%)' }} />
        {/* Top edge highlight */}
        <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/15 to-transparent" />

        <div className="p-7 flex flex-col md:flex-row items-center md:items-start gap-7">
          {/* Avatar */}
          <div className="relative shrink-0">
            <div className="absolute inset-0 rounded-full bg-accent/25 blur-xl scale-110" />
            <div
              className="relative w-24 h-24 rounded-full flex items-center justify-center"
              style={{
                background: 'linear-gradient(135deg, rgba(230,0,0,0.15) 0%, rgba(30,30,30,0.8) 100%)',
                border: '2px solid rgba(230,0,0,0.35)',
                boxShadow: '0 0 0 4px rgba(230,0,0,0.08), 0 8px 24px rgba(0,0,0,0.4)'
              }}
            >
              <User size={34} className="text-white/50" />
            </div>
          </div>

          {/* Info */}
          <div className="flex-1 flex flex-col items-center md:items-start text-center md:text-left gap-1">
            <h2 className="text-2xl font-black tracking-tight leading-none">
              <span className="bg-clip-text text-transparent" style={{ backgroundImage: 'linear-gradient(90deg, #ff3a3a 0%, rgba(230,0,0,0.75) 100%)' }}>
                {profile.username}
              </span>
            </h2>

            {(profile.firstname || profile.lastname) && (
              <p className="text-white/55 text-sm font-medium">
                {[profile.firstname, profile.lastname].filter(Boolean).join(' ')}
              </p>
            )}

            {profile.bio ? (
              <p className="text-white/40 text-[13px] italic leading-relaxed mt-1 max-w-lg line-clamp-2 border-l-2 border-accent/30 pl-3">
                {profile.bio}
              </p>
            ) : (
              <p className="text-white/30 text-sm font-light mt-1">Content Creator on KeyFlicks</p>
            )}

            {/* Stats row */}
            <div className="flex flex-wrap items-center gap-3 mt-4">
              <div
                className="flex items-center gap-2 px-4 py-2 rounded-full"
                style={{
                  background: 'rgba(230,0,0,0.08)',
                  border: '1px solid rgba(230,0,0,0.2)',
                  boxShadow: '0 0 12px rgba(230,0,0,0.1) inset'
                }}
              >
                <FileVideo size={13} className="text-accent/80" />
                <span className="text-sm font-bold text-white">{profile.videos_uploaded ?? 0}</span>
                <span className="text-[10px] text-white/40 uppercase tracking-widest font-semibold">Videos</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* ── Videos Section ── */}
      <div className="mt-4 flex justify-between items-end mb-2 px-2">
         <div>
            <h3 className="text-xl font-bold text-white mb-1">Public Library</h3>
            <p className="text-xs text-white/40 uppercase tracking-widest font-semibold flex items-center gap-2">
              <div className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse"></div>
              {profile.username}'s Uploads
            </p>
         </div>
      </div>

      <motion.div layout className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
         <AnimatePresence mode="popLayout">
         {status === 'success' && data.pages.flatMap(p => p.videos || []).map(video => (
               <motion.div
                 layout
                 initial={{ opacity: 0, scale: 0.9 }}
                 animate={{ opacity: 1, scale: 1 }}
                 exit={{ opacity: 0, scale: 0.8, filter: 'blur(8px)' }}
                 transition={{ layout: { type: 'spring', stiffness: 300, damping: 30 }, duration: 0.2 }}
                 key={video.id}
                 className="flex flex-col justify-between border rounded-2xl p-5 relative overflow-hidden group transition-colors hover:bg-white/5"
                 style={{
                   background: 'rgba(255,255,255,0.02)',
                   borderColor: 'rgba(255,255,255,0.06)',
                 }}
               >
                  <div className="mb-6">
                     <div className="flex justify-between items-start mb-3">
                        <h4 className="font-bold text-white text-base truncate">{video.title || video.id}</h4>
                     </div>
                     <p className="text-xs text-white/40 line-clamp-2 leading-relaxed font-light">
                        {video.description || 'No description provided.'}
                     </p>
                     <div
                        className="mt-2.5 flex items-center gap-2 cursor-pointer group/copy bg-white/[0.03] hover:bg-white/[0.06] px-2.5 py-1.5 rounded-lg transition-colors w-fit max-w-full"
                        onClick={() => { navigator.clipboard.writeText(video.id); toast.success('Video ID copied!'); }}
                        title="Click to copy Video ID"
                     >
                        <span className="text-[10px] text-cyan-400/60 font-mono truncate">{video.id}</span>
                        <Copy size={10} className="text-cyan-400/30 group-hover/copy:text-cyan-400/70 transition-colors shrink-0" />
                     </div>
                     {video.created_at && (
                        <div className="mt-2 text-[10px] text-white/30 font-medium">
                           {formatDistanceToNow(new Date(video.created_at), { addSuffix: true })}
                        </div>
                     )}
                  </div>

                  <div className="flex gap-3 mt-auto">
                      <Link
                        href={`/watch/${video.id}`}
                        className="flex-1 flex items-center justify-center gap-2 bg-white/10 hover:bg-white/20 text-white py-2.5 rounded-xl text-xs font-semibold transition-colors"
                      >
                         <Play size={14} fill="currentColor" /> Watch
                      </Link>
                  </div>
               </motion.div>
         ))}
         </AnimatePresence>
      </motion.div>

      {/* Loading States & Observer Target */}
      <div ref={observerTarget} className="w-full py-8 flex justify-center mt-4">
         {isFetchingNextPage ? (
            <div className="flex items-center gap-2 text-white/50 text-sm">
               <Loader2 className="animate-spin" size={16} /> Loading content...
            </div>
         ) : hasNextPage ? (
            <div className="text-white/20 text-xs tracking-widest uppercase">Scroll for more</div>
         ) : status === 'success' && data.pages.flatMap(p => p.videos || []).length > 0 ? (
            <div className="flex flex-col items-center">
              <div className="w-1 h-1 rounded-full bg-white/20 mb-2"></div>
              <p className="text-white/20 text-[10px] tracking-widest uppercase font-bold">End of Catalogue</p>
            </div>
         ) : null}

         {status === 'success' && data.pages.flatMap(p => p.videos || []).length === 0 && (
             <div className="flex flex-col items-center justify-center py-20 w-full text-center">
                <Video size={48} className="text-white/10 mb-4" />
                <h3 className="text-white/60 font-medium mb-2">No Public Uploads</h3>
                <p className="text-white/30 text-sm font-light max-w-sm">This creator hasn&apos;t published any streamable content yet.</p>
             </div>
         )}
      </div>

    </div>
  );
}
