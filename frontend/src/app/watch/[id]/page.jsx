'use client';
import { useParams, useRouter } from 'next/navigation';
import VideoPlayer from '@/components/VideoPlayer';
import InteractionsBar from '@/components/InteractionsBar';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { AlertCircle, Loader2 } from 'lucide-react';
import Link from 'next/link';

export default function WatchPage() {
  const params = useParams();
  const videoId = params.id;
  const router = useRouter();

  const { data: statusData, isLoading, error } = useQuery({
    queryKey: ['video-status', videoId],
    queryFn: async () => {
      const res = await api.get(`/status/${videoId}`);
      return res.data;
    },
    retry: false
  });

  if (isLoading) {
    return (
      <div className="w-full min-h-[50vh] flex flex-col items-center justify-center gap-4">
         <Loader2 size={48} className="animate-spin text-accent" />
         <p className="text-[#aaa]">Verifying video availability...</p>
      </div>
    );
  }

  if (error || statusData?.status === 'not_found' || !statusData) {
    return (
      <div className="w-full min-h-[50vh] flex flex-col items-center justify-center gap-4 bg-surface-1/50 rounded-xl border border-border p-8">
         <AlertCircle size={48} className="text-[#ff5252]" />
         <h2 className="text-2xl font-bold text-white">Video Not Found</h2>
         <p className="text-[#aaa] text-center max-w-md">The video ID you entered does not exist or has been removed from our servers.</p>
         <Link href="/" className="mt-4 bg-surface-2 hover:bg-border px-6 py-2 rounded-full transition-colors">Go Back Home</Link>
      </div>
    );
  }

  if (statusData.status === 'processing') {
    return (
      <div className="w-full min-h-[50vh] flex flex-col items-center justify-center gap-4 bg-surface-1/50 rounded-xl border border-border p-8">
         <Loader2 size={48} className="animate-spin text-yellow-500" />
         <h2 className="text-2xl font-bold text-white">Video Processing</h2>
         <p className="text-[#aaa] text-center max-w-md">This video is currently being transcoded to multiple quality formats. Please check back later.</p>
         <Link href="/" className="mt-4 bg-surface-2 hover:bg-border px-6 py-2 rounded-full transition-colors">Go Back Home</Link>
      </div>
    );
  }

  // Ensure 'ready' state or anything else proceeds to play
  return (
    <div className="w-full flex flex-col gap-2 relative">
       <div className="w-full">
           <VideoPlayer videoId={videoId} />
       </div>
       <div className="w-full max-w-5xl self-center">
           <InteractionsBar videoId={videoId} />
       </div>
    </div>
  );
}
