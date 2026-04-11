'use client';
import { useEffect, useRef, useCallback } from 'react';
import { api } from '@/lib/api';
import { useAuthStore } from '@/lib/store';
import { useInfiniteQuery, useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { useRouter } from 'next/navigation';
import { User, Trash2, Play, UploadCloud, Loader2 } from 'lucide-react';
import Link from 'next/link';

export default function DashboardPage() {
  const { isAuthenticated } = useAuthStore();
  const router = useRouter();
  const queryClient = useQueryClient();
  const observerTarget = useRef(null);

  useEffect(() => {
    if (!isAuthenticated) router.push('/login');
  }, [isAuthenticated, router]);

  // Fetch current user details
  const { data: profile } = useQuery({
    queryKey: ['profile', 'me'],
    queryFn: async () => {
      const res = await api.get('/profile/me');
      return res.data;
    },
    enabled: isAuthenticated
  });

  // Fetch paginated videos
  const {
    data,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    status
  } = useInfiniteQuery({
    queryKey: ['my-videos'],
    queryFn: async ({ pageParam }) => {
      const p = new URLSearchParams();
      if (pageParam?.cursor_time) p.append('cursor_time', pageParam.cursor_time);
      if (pageParam?.cursor_id) p.append('cursor_id', pageParam.cursor_id);
      
      const res = await api.get(`/my-videos?${p.toString()}`);
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
    enabled: isAuthenticated,
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

  // Delete Mutation
  const deleteVideoMutation = useMutation({
    mutationFn: async (videoId) => api.delete(`/video/${videoId}`),
    onSuccess: () => {
      toast.success("Video securely deleted");
      queryClient.invalidateQueries({ queryKey: ['my-videos'] });
    },
    onError: (err) => {
      toast.error(err.response?.data?.error || "Failed to delete video");
    }
  });

  if (!isAuthenticated) return null;

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
        <div className="absolute -top-px left-1/2 -translate-x-1/2 w-1/3 h-px bg-gradient-to-r from-transparent via-white/20 to-transparent rounded-full" />
        
        <div className="w-24 h-24 rounded-full bg-white/5 border border-white/10 flex items-center justify-center shrink-0 shadow-inner">
           {/* Placeholder for Profile Picture */}
           <User size={36} className="text-white/40" />
        </div>
        
        <div className="flex-1 flex flex-col items-center md:items-start text-center md:text-left">
           <h2 className="text-2xl font-black text-white drop-shadow-md mb-1 pb-1">
             <span className="bg-clip-text text-transparent bg-gradient-to-r from-white to-white/70">
                {profile?.username || "Loading..."}
             </span>
           </h2>
           <p className="text-white/40 text-sm font-mono mb-4">{profile?.email}</p>
           
           <div className="flex gap-4">
              <Link 
                href="/profile" 
                className="px-4 py-2 rounded-xl text-xs font-semibold text-white/70 bg-white/5 hover:bg-white/10 hover:text-white border border-white/10 transition-all"
              >
                Edit Profile
              </Link>
           </div>
        </div>
      </div>

      {/* ── Videos Section ── */}
      <div className="mt-4 flex justify-between items-end mb-2 px-2">
         <div>
            <h3 className="text-xl font-bold text-white mb-1">Your secure library</h3>
            <p className="text-xs text-white/40 uppercase tracking-widest font-semibold flex items-center gap-2">
              <div className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse"></div>
              Encrypted Vault
            </p>
         </div>
         <Link 
           href="/upload" 
           className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-sm font-bold text-white transition-all hover:-translate-y-0.5 active:translate-y-0"
           style={{ background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)', boxShadow: '0 4px 20px rgba(255,0,0,0.3)' }}
         >
           <UploadCloud size={16} /> Upload New
         </Link>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
         {status === 'success' && data.pages.map((page, i) => (
            page.videos?.map(video => (
               <div 
                 key={video.id} 
                 className="flex flex-col justify-between border rounded-2xl p-5 relative overflow-hidden group transition-all hover:bg-white/5"
                 style={{
                   background: 'rgba(255,255,255,0.02)',
                   borderColor: 'rgba(255,255,255,0.06)',
                 }}
               >
                  <div className="mb-6">
                     <div className="flex justify-between items-start mb-3">
                        <h4 className="font-bold text-white text-base truncate flex-1 pr-4">{video.title || video.id}</h4>
                        <span className={`text-[10px] uppercase tracking-wider font-bold px-2 py-1 rounded-md ${video.transcoding_status === 'ready' ? 'bg-green-500/10 text-green-400' : 'bg-yellow-500/10 text-yellow-400'}`}>
                           {video.transcoding_status}
                        </span>
                     </div>
                     <p className="text-xs text-white/40 line-clamp-2 leading-relaxed font-light">
                        {video.description || "No description provided."}
                     </p>
                  </div>
                  
                  <div className="flex gap-3 mt-auto">
                      <Link 
                        href={`/watch/${video.id}`} 
                        className="flex-1 flex items-center justify-center gap-2 bg-white/10 hover:bg-white/20 text-white py-2.5 rounded-xl text-xs font-semibold transition-colors"
                      >
                         <Play size={14} fill="currentColor" /> Watch
                      </Link>
                      <button 
                        onClick={() => deleteVideoMutation.mutate(video.id)}
                        disabled={deleteVideoMutation.isPending}
                        className="flex items-center justify-center bg-red-500/10 hover:bg-red-500/20 text-red-500 w-10 rounded-xl transition-colors disabled:opacity-50"
                      >
                          <Trash2 size={14} />
                      </button>
                  </div>
               </div>
            ))
         ))}
      </div>

      {/* Loading States & Observer Target */}
      <div ref={observerTarget} className="w-full py-8 flex justify-center mt-4">
         {isFetchingNextPage ? (
            <div className="flex items-center gap-2 text-white/50 text-sm">
               <Loader2 className="animate-spin" size={16} /> Loading more securely...
            </div>
         ) : hasNextPage ? (
            <div className="text-white/20 text-xs tracking-widest uppercase">Scroll for more</div>
         ) : status === 'success' && data.pages[0]?.videos?.length > 0 ? (
            <div className="flex flex-col items-center">
              <div className="w-1 h-1 rounded-full bg-white/20 mb-2"></div>
              <p className="text-white/20 text-[10px] tracking-widest uppercase font-bold">End of Library</p>
            </div>
         ) : null}
         
         {status === 'success' && data.pages[0]?.videos?.length === 0 && (
             <div className="flex flex-col items-center justify-center py-20 w-full text-center">
                <Video size={48} className="text-white/10 mb-4" />
                <h3 className="text-white/60 font-medium mb-2">Vault is Empty</h3>
                <p className="text-white/30 text-sm font-light max-w-sm">You haven't uploaded any secure videos yet. Click "Upload New" to store your premium content.</p>
             </div>
         )}
      </div>

    </div>
  );
}
