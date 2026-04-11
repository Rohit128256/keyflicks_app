'use client';
import { useState, useEffect } from 'react';
import { api } from '@/lib/api';
import { useAuthStore } from '@/lib/store';
import { ThumbsUp, Trash2, Send } from 'lucide-react';
import toast from 'react-hot-toast';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';

export default function InteractionsBar({ videoId }) {
  const { isAuthenticated, user } = useAuthStore();
  const queryClient = useQueryClient();

  const [commentText, setCommentText] = useState('');

  // Fetch likes count
  const { data: likesData, refetch: refetchLikes } = useQuery({
    queryKey: ['likes', videoId],
    queryFn: async () => {
      const res = await api.get(`/likes?video_id=${videoId}`);
      return res.data;
    }
  });

  // Fetch comments
  const { data: commentsData, refetch: refetchComments } = useQuery({
    queryKey: ['comments', videoId],
    queryFn: async () => {
      const res = await api.get(`/comments?video_id=${videoId}&cursor=`);
      return res.data;
    }
  });

  const toggleLikeMutation = useMutation({
    mutationFn: async (action) => {
      return api.post(`/like?video_id=${videoId}&action=${action}`);
    },
    onSuccess: () => {
      refetchLikes();
    }
  });

  const postCommentMutation = useMutation({
    mutationFn: async ({ text, parentId }) => {
      return api.post(`/comment`, { video_id: videoId, text, parent_id: parentId });
    },
    onSuccess: () => {
      setCommentText('');
      refetchComments();
      toast.success("Comment posted!");
    }
  });

  const deleteCommentMutation = useMutation({
    mutationFn: async (commentId) => {
      return api.delete(`/delcomment`, { data: { comment_id: commentId, video_id: videoId } });
    },
    onSuccess: () => {
      refetchComments();
      toast.success("Comment deleted!");
    }
  });

  const handleLikeToggle = () => {
    if (!isAuthenticated) return toast.error("Please login first");
    // Assuming you don't know the exact current interaction state easily without deeper API integration
    // Here we'll default to 'like' for the action per the spec, adjust as needed.
    toggleLikeMutation.mutate('like');
  };

  const handlePostComment = (e) => {
    e.preventDefault();
    if (!isAuthenticated) return toast.error("Please login first");
    if (!commentText.trim()) return;
    postCommentMutation.mutate({ text: commentText, parentId: "" });
  };

  return (
    <div className="w-full bg-surface-1 p-6 rounded-xl mt-6 border border-border">
       <div className="flex items-center justify-between border-b border-border pb-4 mb-4">
          <h2 className="text-xl font-bold">Interactions</h2>
          <button 
             onClick={handleLikeToggle}
             className="flex items-center gap-2 bg-surface-2 hover:bg-[rgba(255,255,255,0.1)] px-4 py-2 rounded-full transition-colors"
          >
             <ThumbsUp size={18} /> {likesData?.count || 0} Likes
          </button>
       </div>

       <div className="comments-section mt-6">
          <h3 className="text-lg font-medium mb-4">Comments</h3>
          
          <form onSubmit={handlePostComment} className="flex items-center gap-3 mb-6">
             <input 
               type="text" 
               placeholder="Add a comment..." 
               value={commentText}
               onChange={(e) => setCommentText(e.target.value)}
               className="flex-1 bg-[#1c1c1c] p-3 rounded-xl border border-border text-white focus:outline-none focus:border-accent"
             />
             <button 
                type="submit" 
                disabled={postCommentMutation.isPending}
                className="bg-accent hover:bg-accent-hover text-white p-3 rounded-xl transition-colors disabled:opacity-50"
             >
                <Send size={18} />
             </button>
          </form>

          <div className="flex flex-col gap-4">
             {commentsData?.comments?.map((comment) => (
                <div key={comment.id} className="bg-surface-2 p-4 rounded-xl flex items-start justify-between">
                   <div>
                       <div className="font-bold text-sm mb-1">{comment.username || "User"}</div>
                       <div className="text-[#ddd] text-sm">{comment.text}</div>
                   </div>
                   {isAuthenticated && (
                     <button onClick={() => deleteCommentMutation.mutate(comment.id)} className="text-[#aaa] hover:text-[#ff5252] transition-colors p-1">
                        <Trash2 size={16} />
                     </button>
                   )}
                </div>
             ))}
             {!commentsData?.comments?.length && (
                 <div className="text-center text-[#aaa] text-sm py-4">No comments yet. Be the first to start the discussion!</div>
             )}
          </div>
       </div>
    </div>
  );
}
