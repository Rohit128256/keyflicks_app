'use client';
import { useEffect, useRef, use } from 'react';
import { api } from '@/lib/api';
import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { User, Play, Video, Loader2, Copy } from 'lucide-react';
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
            <p className="text-white/40">The user "{username}" does not exist or has locked their profile.</p>
            <Link href="/" className="inline-block mt-6 px-6 py-2 bg-white/10 hover:bg-white/20 rounded-xl transition-colors">Return Home</Link>
         </div>
      </div>
    );
  }

  return (
    <div className="w-full flex flex-col gap-6 w-full max-w-6xl mx-auto py-4">
      {/* ── Top Profile Summary ── */}
      <div 
        className="w-full border rounded-3xl p-6 relative overflow-hidden flex flex-col md:flex-row items-center md:items-start gap-6 z-10"
        style={{
          background: 'rgba(255,255,255,0.02)',
          backdropFilter: 'blur(24px)',
          borderColor: 'rgba(255,255,255,0.08)',
          boxShadow: '0 0 0 1px rgba(255,255,255,0.04) inset, 0 10px 40px rgba(0,0,0,0.3)'
        }}
      >
        <div className="absolute -top-px left-1/2 -translate-x-1/2 w-1/3 h-px bg-gradient-to-r from-transparent via-accent/40 to-transparent rounded-full" />
        
        <div className="w-24 h-24 rounded-full bg-white/5 border border-white/10 flex items-center justify-center shrink-0 shadow-inner">
           {/* Placeholder for Profile Picture */}
           <User size={36} className="text-white/60" />
        </div>
        
        <div className="flex-1 flex flex-col items-center md:items-start text-center md:text-left">
           <h2 className="text-2xl font-black text-white drop-shadow-md mb-1 pb-1">
             <span className="bg-clip-text text-transparent bg-gradient-to-r from-accent to-accent/70">
                {profile.username}
             </span>
           </h2>
           <p className="text-white/40 text-sm font-light mb-4">Content Creator on KeyFlicks</p>
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
