'use client';
import { useState } from 'react';
import { useParams } from 'next/navigation';
import VideoPlayer from '@/components/VideoPlayer';
import InteractionsBar from '@/components/InteractionsBar';
import { LikeDislikeButtons } from '@/components/InteractionsBar';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { AlertCircle, Loader2, Clock, ChevronDown, Copy, Check } from 'lucide-react';
import Link from 'next/link';
import { formatDistanceToNow } from 'date-fns';

// Parses plain text and turns URLs into clickable <a> tags
function renderDescription(text) {
  const urlRegex = /(https?:\/\/[^\s]+)/g;
  const parts = text.split(urlRegex);
  return parts.map((part, i) =>
    urlRegex.test(part) ? (
      <a
        key={i}
        href={part}
        target="_blank"
        rel="noopener noreferrer"
        onClick={(e) => e.stopPropagation()}
        className="text-accent underline underline-offset-2 hover:text-accent/80 transition-colors break-all"
      >
        {part}
      </a>
    ) : (
      part
    )
  );
}

export default function WatchPage() {
  const params = useParams();
  const videoId = params.id;
  const [descExpanded, setDescExpanded] = useState(false);
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(videoId);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

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
      <div className="w-full min-h-[60vh] flex flex-col items-center justify-center gap-4">
         <div className="w-12 h-12 rounded-full border-4 border-white/10 border-t-accent animate-spin"></div>
         <p className="text-white/40 text-sm font-medium">Loading video...</p>
      </div>
    );
  }

  if (error || statusData?.status === 'not_found' || !statusData) {
    return (
      <div className="w-full min-h-[60vh] flex flex-col items-center justify-center gap-6">
         <div className="w-20 h-20 rounded-full bg-red-500/10 flex items-center justify-center">
            <AlertCircle size={40} className="text-red-500" />
         </div>
         <div className="text-center">
            <h2 className="text-2xl font-bold text-white mb-2">Video Not Found</h2>
            <p className="text-white/40 text-sm max-w-md">The video you're looking for doesn't exist or has been removed.</p>
         </div>
         <Link href="/" className="px-6 py-2.5 bg-white/10 hover:bg-white/15 rounded-xl text-sm font-semibold text-white transition-all">Go Home</Link>
      </div>
    );
  }

  if (statusData.status === 'processing') {
    return (
      <div className="w-full min-h-[60vh] flex flex-col items-center justify-center gap-6">
         <div className="w-16 h-16 rounded-full border-4 border-white/10 border-t-yellow-500 animate-spin"></div>
         <div className="text-center">
            <h2 className="text-2xl font-bold text-white mb-2">Video is Processing</h2>
            <p className="text-white/40 text-sm max-w-md">This video is being transcoded into multiple quality formats. Please check back in a few minutes.</p>
         </div>
         <Link href="/" className="px-6 py-2.5 bg-white/10 hover:bg-white/15 rounded-xl text-sm font-semibold text-white transition-all">Go Home</Link>
      </div>
    );
  }

  return (
    <div className="w-full max-w-5xl mx-auto flex flex-col gap-0 py-4 px-4">
       {/* Video Player */}
       <div className="w-full rounded-2xl overflow-hidden shadow-[0_8px_40px_rgba(0,0,0,0.6)]">
           <VideoPlayer videoId={videoId} />
       </div>

       {/* Video Info — YouTube Style */}
       <div className="mt-5 px-1">
          <h1 className="text-xl md:text-2xl font-bold text-white leading-snug tracking-tight">
             {statusData.title || 'Untitled Video'}
          </h1>
          
          <hr className="border-t border-white/[0.08] my-3" />
          
          <div className="flex items-center gap-2 mt-2 mb-1">
             <span className="text-xs text-white/40 uppercase tracking-widest font-bold">ID</span>
             <code className="text-xs font-mono px-2.5 py-0.5 rounded shadow-inner bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 tracking-wide select-all">
                {videoId}
             </code>
             <button 
                onClick={handleCopy}
                className="p-1 rounded-md text-white/40 hover:text-white hover:bg-white/10 transition-all active:scale-95"
                title="Copy ID"
             >
                {copied ? <Check size={14} className="text-emerald-400" /> : <Copy size={14} />}
             </button>
             {copied && <span className="text-[10px] font-semibold text-emerald-400 tracking-wide">Copied!</span>}
          </div>
          
          <hr className="border-t border-white/[0.08] my-3" />

          {/* Like / Dislike capsule — above description */}
          <div className="mt-1 mb-4">
            <LikeDislikeButtons videoId={videoId} />
          </div>

          {/* Description Card — Collapsible */}
          {statusData.description && (
             <div
                className="mt-3 p-4 bg-white/[0.04] hover:bg-white/[0.06] rounded-xl cursor-pointer transition-colors group"
                onClick={() => setDescExpanded(!descExpanded)}
             >
                <div className="flex items-center gap-2 mb-2">
                   {statusData.created_at && (
                      <span className="text-xs text-white/50 font-medium">
                         {formatDistanceToNow(new Date(statusData.created_at), { addSuffix: true })}
                      </span>
                   )}
                   <ChevronDown
                      size={14}
                      className={`text-white/30 transition-transform duration-300 ml-auto ${descExpanded ? 'rotate-180' : ''}`}
                   />
                </div>

                {/* Grid-rows trick: animates from 0fr → 1fr for butter-smooth expand/collapse */}
                <div
                   className="grid transition-[grid-template-rows] duration-300 ease-in-out"
                   style={{ gridTemplateRows: descExpanded ? '1fr' : '0fr' }}
                >
                   <div className="overflow-hidden">
                      <p className="text-white/60 text-sm leading-relaxed font-light whitespace-pre-wrap pb-1">
                         {renderDescription(statusData.description)}
                      </p>
                   </div>
                </div>

                {/* Collapsed preview — always visible, fades out when expanded */}
                {!descExpanded && (
                   <p className="text-white/60 text-sm leading-relaxed font-light whitespace-pre-wrap line-clamp-2">
                      {renderDescription(statusData.description)}
                   </p>
                )}

                <span className={`text-xs font-semibold mt-2 inline-block transition-colors ${
                   descExpanded ? 'text-accent/60' : 'text-white/30'
                }`}>
                   {descExpanded ? 'Show less' : '...more'}
                </span>
             </div>
          )}

          {/* Fallback: show time separately if no description */}
          {!statusData.description && statusData.created_at && (
             <div className="flex items-center gap-1.5 mt-2 text-white/40 text-xs font-medium">
                <Clock size={12} />
                {formatDistanceToNow(new Date(statusData.created_at), { addSuffix: true })}
             </div>
          )}
       </div>

       {/* Interactions — Likes & Comments */}
       <InteractionsBar videoId={videoId} />
    </div>
  );
}
