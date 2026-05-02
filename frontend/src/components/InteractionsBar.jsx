'use client';
import { useState, useRef, useCallback, useEffect } from 'react';
import { api } from '@/lib/api';
import { useAuthStore } from '@/lib/store';
import { ThumbsUp, ThumbsDown, Send, MessageCircle, ChevronDown, Trash2, Reply, Loader2, User } from 'lucide-react';
import toast from 'react-hot-toast';
import { useQuery, useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { formatDistanceToNow } from 'date-fns';
import Link from 'next/link';
import { motion, AnimatePresence } from 'framer-motion';

// ─── Shared animation variants ───
const commentItemVariants = {
  initial: { opacity: 0, y: 10 },
  animate: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.2, ease: 'easeOut' },
  },
  exit: {
    opacity: 0,
    x: -28,
    transition: { duration: 0.22, ease: 'easeInOut' },
  },
};

// ─── Helper: build a temporary optimistic comment/reply object ───
function buildOptimisticComment(text, author, parentId = null) {
  return {
    id: `optimistic-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    parentId,
    author,
    text,
    replyCounts: 0,
    createdAt: new Date().toISOString(),
    _isOptimistic: true,
  };
}

// ─── Helper: prepend a comment to page 0 of infinite query data ───
function prependToInfiniteCache(old, newItem) {
  if (!old) {
    return {
      pages: [{ comments: [newItem], next_cursor_time: null, next_cursor_id: null }],
      pageParams: [undefined],
    };
  }
  return {
    ...old,
    pages: old.pages.map((page, i) =>
      i === 0 ? { ...page, comments: [newItem, ...(page.comments || [])] } : page
    ),
  };
}

export function LikeDislikeButtons({ videoId }) {
  const { isAuthenticated } = useAuthStore();

  const [liked, setLiked] = useState(false);
  const [disliked, setDisliked] = useState(false);
  const [likeCount, setLikeCount] = useState(0);
  const [dislikeCount, setDislikeCount] = useState(0);
  const [likeAnimating, setLikeAnimating] = useState(false);
  const [dislikeAnimating, setDislikeAnimating] = useState(false);

  const debounceRef = useRef(null);
  const lastSentRef = useRef(null);

  const { data: likesData } = useQuery({
    queryKey: ['likes', videoId],
    queryFn: async () => {
      const res = await api.get(`/likes?video_id=${videoId}`);
      return res.data;
    },
    enabled: !!videoId,
  });

  useEffect(() => {
    if (likesData) {
      setLiked(likesData.currUserLiked ?? false);
      setDisliked(likesData.currUserDisliked ?? false);
      setLikeCount(likesData.videoLikes ?? 0);
      setDislikeCount(likesData.VideoDislikes ?? 0);
      if (likesData.currUserLiked) lastSentRef.current = 'like';
      else if (likesData.currUserDisliked) lastSentRef.current = 'dislike';
      else lastSentRef.current = 'unlike';
    }
  }, [likesData]);

  const scheduleApiCall = useCallback((action) => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      if (action !== lastSentRef.current) {
        lastSentRef.current = action;
        api.post(`/like?video_id=${videoId}&action=${action}`).catch(() => {
          toast.error('Failed to update');
        });
      }
    }, 500);
  }, [videoId]);

  const handleLike = useCallback(() => {
    if (!isAuthenticated) return toast.error('Please login first');
    const wasLiked = liked;
    const wasDisliked = disliked;
    if (wasLiked) {
      setLiked(false);
      setLikeCount(c => Math.max(0, c - 1));
      setLikeAnimating(true);
      setTimeout(() => setLikeAnimating(false), 300);
      scheduleApiCall('unlike');
    } else {
      setLiked(true);
      setLikeCount(c => c + 1);
      setLikeAnimating(true);
      setTimeout(() => setLikeAnimating(false), 300);
      if (wasDisliked) {
        setDisliked(false);
        setDislikeCount(c => Math.max(0, c - 1));
      }
      scheduleApiCall('like');
    }
  }, [videoId, isAuthenticated, liked, disliked, scheduleApiCall]);

  const handleDislike = useCallback(() => {
    if (!isAuthenticated) return toast.error('Please login first');
    const wasLiked = liked;
    const wasDisliked = disliked;
    if (wasDisliked) {
      setDisliked(false);
      setDislikeCount(c => Math.max(0, c - 1));
      setDislikeAnimating(true);
      setTimeout(() => setDislikeAnimating(false), 300);
      scheduleApiCall('undislike');
    } else {
      setDisliked(true);
      setDislikeCount(c => c + 1);
      setDislikeAnimating(true);
      setTimeout(() => setDislikeAnimating(false), 300);
      if (wasLiked) {
        setLiked(false);
        setLikeCount(c => Math.max(0, c - 1));
      }
      scheduleApiCall('dislike');
    }
  }, [videoId, isAuthenticated, liked, disliked, scheduleApiCall]);

  return (
    // ── Single capsule pill (YouTube style) ──
    <div className="inline-flex items-center rounded-full border border-white/[0.12] bg-white/[0.06] overflow-hidden select-none">
      {/* Like half */}
      <button
        onClick={handleLike}
        className={`flex items-center gap-2 px-5 py-2 text-sm font-semibold transition-all duration-200
          ${liked
            ? 'bg-accent/20 text-accent'
            : 'text-white/60 hover:bg-white/10 hover:text-white'
          }`}
      >
        <ThumbsUp
          size={17}
          fill={liked ? 'currentColor' : 'none'}
          className={`transition-transform duration-300 ${likeAnimating ? 'scale-125' : 'scale-100'}`}
        />
        <span className="font-bold tabular-nums">{likeCount}</span>
      </button>

      {/* Central divider */}
      <div className="w-px self-stretch bg-white/[0.12]" />

      {/* Dislike half */}
      <button
        onClick={handleDislike}
        className={`flex items-center gap-2 px-5 py-2 text-sm font-semibold transition-all duration-200
          ${disliked
            ? 'bg-red-500/20 text-red-400'
            : 'text-white/60 hover:bg-white/10 hover:text-white'
          }`}
      >
        <ThumbsDown
          size={17}
          fill={disliked ? 'currentColor' : 'none'}
          className={`transition-transform duration-300 ${dislikeAnimating ? 'scale-125' : 'scale-100'}`}
        />
        <span className="font-bold tabular-nums">{dislikeCount}</span>
      </button>
    </div>
  );
}

// ─── Single Comment Component ───
function CommentItem({ comment, videoId, currentUserId }) {
  const [showReplies, setShowReplies] = useState(false);
  const [showReplyInput, setShowReplyInput] = useState(false);
  const [replyText, setReplyText] = useState('');
  const queryClient = useQueryClient();

  // Fetch replies when expanded
  const {
    data: repliesData,
    fetchNextPage: fetchMoreReplies,
    hasNextPage: hasMoreReplies,
    isFetchingNextPage: isFetchingMoreReplies,
    isFetching: isLoadingReplies,
  } = useInfiniteQuery({
    queryKey: ['replies', videoId, comment.id],
    queryFn: async ({ pageParam }) => {
      const p = new URLSearchParams();
      p.append('video_id', videoId);
      p.append('parent_id', comment.id);
      if (pageParam?.cursor_time) p.append('cursor_time', pageParam.cursor_time);
      if (pageParam?.cursor_id) p.append('cursor_id', pageParam.cursor_id);
      const res = await api.get(`/comments?${p.toString()}`);
      return res.data;
    },
    getNextPageParam: (lastPage) => {
      if (lastPage.next_cursor_time && lastPage.next_cursor_id) {
        return { cursor_time: lastPage.next_cursor_time, cursor_id: lastPage.next_cursor_id };
      }
      return undefined;
    },
    enabled: showReplies,
    staleTime: 1000 * 10,
  });

  const replies = repliesData?.pages.flatMap(p => p.comments || []) || [];

  // ── Post reply mutation — with optimistic update ──────────────────────────
  const postReplyMutation = useMutation({
    mutationFn: async (text) => {
      return api.post('/comment', { video_id: videoId, parent_id: comment.id, text });
    },

    onMutate: async (text) => {
      // 1. Read author info from the already-fetched profile cache (no extra network call)
      const profileData = queryClient.getQueryData(['profile', 'me']);
      const author = {
        userId: profileData?.userid || '',
        username: profileData?.username || 'You',
      };
      const optimisticReply = buildOptimisticComment(text, author, comment.id);

      // 2. Cancel any in-flight refetch for this reply list so it won't overwrite our optimistic update
      await queryClient.cancelQueries({ queryKey: ['replies', videoId, comment.id] });
      const previousReplies = queryClient.getQueryData(['replies', videoId, comment.id]);

      // 3. Instantly prepend the optimistic reply to the list
      queryClient.setQueryData(
        ['replies', videoId, comment.id],
        (old) => prependToInfiniteCache(old, optimisticReply)
      );

      // 4. Optimistically increment this parent comment's replyCounts in ALL caches
      queryClient.setQueriesData(
        { predicate: (query) => query.queryKey[0] === 'comments' || query.queryKey[0] === 'replies' },
        (old) => {
          if (!old || !old.pages) return old;
          return {
            ...old,
            pages: old.pages.map((page) => ({
              ...page,
              comments: page.comments?.map((c) =>
                c.id === comment.id ? { ...c, replyCounts: c.replyCounts + 1 } : c
              ) ?? [],
            })),
          };
        }
      );

      // 5. Show the reply section and close the reply input immediately
      setShowReplies(true);
      setShowReplyInput(false);
      setReplyText('');

      return { previousReplies };
    },

    onSuccess: () => {
      toast.success('Reply posted!');
      // Delayed invalidate — gives the Redis stream worker ~2.5s to write to DB.
      // Invalidating comment-count also resets the 45s polling interval from this point.
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['replies', videoId, comment.id] });
        queryClient.invalidateQueries({ queryKey: ['comments', videoId] });
        queryClient.invalidateQueries({ queryKey: ['comment-count', videoId] });
      }, 2500);
    },

    onError: (err, text, context) => {
      // Rollback: restore previous reply list
      if (context?.previousReplies !== undefined) {
        queryClient.setQueryData(['replies', videoId, comment.id], context.previousReplies);
      }
      // Rollback: undo the replyCounts increment globally
      queryClient.setQueriesData(
        { predicate: (query) => query.queryKey[0] === 'comments' || query.queryKey[0] === 'replies' },
        (old) => {
          if (!old || !old.pages) return old;
          return {
            ...old,
            pages: old.pages.map((page) => ({
              ...page,
              comments: page.comments?.map((c) =>
                c.id === comment.id ? { ...c, replyCounts: Math.max(0, c.replyCounts - 1) } : c
              ) ?? [],
            })),
          };
        }
      );
      // Restore text so user can retry
      setReplyText(text);
      setShowReplyInput(true);
      toast.error('Failed to post reply. Please try again.');
    },
  });

  // ── Delete mutation — optimistic removal ──────────────────────────────────
  const deleteCommentMutation = useMutation({
    mutationFn: async () => {
      return api.delete('/delcomment', { data: { comment_id: comment.id, video_id: videoId } });
    },
    onSuccess: () => {
      const isReply = !!comment.parentId;

      if (isReply) {
        // Instantly remove this reply from the parent's replies cache
        queryClient.setQueryData(['replies', videoId, comment.parentId], (old) => {
          if (!old) return old;
          return {
            ...old,
            pages: old.pages.map(page => ({
              ...page,
              comments: page.comments?.filter(c => c.id !== comment.id) ?? [],
            })),
          };
        });

        // Decrement parent's replyCounts by exactly 1 in ALL nested caches mapping to this domain
        queryClient.setQueriesData(
          { predicate: (query) => query.queryKey[0] === 'comments' || query.queryKey[0] === 'replies' },
          (old) => {
            if (!old || !old.pages) return old;
            return {
              ...old,
              pages: old.pages.map((page) => ({
                ...page,
                comments: page.comments?.map((c) =>
                  c.id === comment.parentId
                    ? { ...c, replyCounts: Math.max(0, c.replyCounts - 1) }
                    : c
                ) ?? [],
              })),
            };
          }
        );
      } else {
        // Instantly remove from top-level comments cache
        queryClient.setQueryData(['comments', videoId], (old) => {
          if (!old) return old;
          return {
            ...old,
            pages: old.pages.map(page => ({
              ...page,
              comments: page.comments?.filter(c => c.id !== comment.id) ?? [],
            })),
          };
        });

        // Evict loaded replies for this comment (all children are deleted in DB too)
        queryClient.removeQueries({ queryKey: ['replies', videoId, comment.id] });
      }

      // Refresh real count after backend stream worker processes the delete (~2.5s).
      // Invalidating also resets the 45s polling interval from this point.
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['comment-count', videoId] });
      }, 2500);

      toast.success(isReply ? 'Reply deleted' : 'Comment deleted');
    },
    onError: () => toast.error('Failed to delete comment'),
  });

  const handleSubmitReply = (e) => {
    e.preventDefault();
    if (!replyText.trim()) return;
    postReplyMutation.mutate(replyText.trim());
  };

  return (
    <div className="flex gap-3 group">
      {/* Avatar */}
      <Link
        href={currentUserId && comment.author?.userId === currentUserId ? '/dashboard' : `/profile/${comment.author?.username}`}
        className="shrink-0"
      >
        <div className="w-9 h-9 rounded-full bg-white/[0.06] border border-white/10 flex items-center justify-center mt-0.5">
          <User size={16} className="text-white/30" />
        </div>
      </Link>

      <div className="flex-1 min-w-0">
        {/* Header */}
        <div className="flex items-center gap-2 mb-1 flex-wrap">
          <Link
            href={currentUserId && comment.author?.userId === currentUserId ? '/dashboard' : `/profile/${comment.author?.username}`}
            className="text-sm font-semibold text-white/80 hover:text-accent transition-colors"
          >
            {comment.author?.username || 'Unknown'}
          </Link>
          <span className="text-[11px] text-white/25">
            {comment.createdAt &&
              formatDistanceToNow(new Date(comment.createdAt), { addSuffix: true })}
          </span>
          {/* Subtle "Sending" pill shown only on optimistic (temp) comments */}
          {comment._isOptimistic && (
            <span className="flex items-center gap-1 text-[10px] text-white/30 font-medium">
              <span className="w-1.5 h-1.5 rounded-full bg-accent/50 animate-pulse" />
              Sending…
            </span>
          )}
        </div>

        {/* Text — slightly dimmer while optimistic */}
        <p
          className={`text-sm leading-relaxed font-light transition-colors duration-300 ${
            comment._isOptimistic ? 'text-white/40' : 'text-white/60'
          }`}
        >
          {comment.text}
        </p>

        {/* Actions — hide on optimistic comments (no real id yet) */}
        {!comment._isOptimistic && (
          <div className="flex items-center gap-4 mt-2">
            <button
              onClick={() => setShowReplyInput(!showReplyInput)}
              className="flex items-center gap-1.5 text-[11px] text-white/30 hover:text-white/60 font-semibold uppercase tracking-wider transition-colors"
            >
              <Reply size={12} /> Reply
            </button>

            {/* Delete — only shown for own comments */}
            {currentUserId && comment.author?.userId === currentUserId && (
              <button
                onClick={() => deleteCommentMutation.mutate()}
                disabled={deleteCommentMutation.isPending}
                className="flex items-center gap-1 text-[11px] text-white/20 hover:text-red-500 font-medium transition-colors disabled:opacity-40"
              >
                {deleteCommentMutation.isPending ? (
                  <Loader2 size={11} className="animate-spin" />
                ) : (
                  <Trash2 size={11} />
                )}
                {deleteCommentMutation.isPending ? 'Deleting…' : 'Delete'}
              </button>
            )}
          </div>
        )}

        {/* Reply Input */}
        {showReplyInput && !comment._isOptimistic && (
          <form onSubmit={handleSubmitReply} className="flex items-center gap-2 mt-3">
            <input
              type="text"
              placeholder="Write a reply..."
              value={replyText}
              onChange={(e) => setReplyText(e.target.value)}
              className="flex-1 bg-white/[0.04] px-4 py-2.5 rounded-xl border border-white/[0.08] text-white text-sm focus:outline-none focus:border-accent/40 transition-colors placeholder:text-white/20"
              autoFocus
            />
            <button
              type="submit"
              disabled={postReplyMutation.isPending || !replyText.trim()}
              className="p-2.5 rounded-xl bg-accent/20 text-accent hover:bg-accent/30 transition-colors disabled:opacity-40"
            >
              {postReplyMutation.isPending ? (
                <Loader2 size={14} className="animate-spin" />
              ) : (
                <Send size={14} />
              )}
            </button>
            <button
              type="button"
              onClick={() => setShowReplyInput(false)}
              className="text-[11px] text-white/30 hover:text-white/60 px-2 py-2"
            >
              Cancel
            </button>
          </form>
        )}

        {/* View Replies Toggle */}
        {comment.replyCounts > 0 && !comment._isOptimistic && (
          <button
            onClick={() => setShowReplies(!showReplies)}
            className="flex items-center gap-1.5 mt-3 text-xs text-accent font-semibold hover:text-accent/80 transition-colors"
          >
            <ChevronDown
              size={14}
              className={`transition-transform duration-200 ${showReplies ? 'rotate-180' : ''}`}
            />
            {showReplies ? 'Hide' : 'View'} {comment.replyCounts}{' '}
            {comment.replyCounts === 1 ? 'reply' : 'replies'}
          </button>
        )}

        {/* Replies List — animated */}
        {showReplies && (
          <div className="mt-3 flex flex-col gap-4 pl-1 border-l-2 border-white/[0.04] ml-1">
            {isLoadingReplies && replies.length === 0 && (
              <div className="flex items-center gap-2 text-white/30 text-xs py-2 pl-4">
                <Loader2 size={12} className="animate-spin" /> Loading replies...
              </div>
            )}

            <AnimatePresence mode="popLayout">
              {replies.map(reply => (
                <motion.div
                  key={reply.id}
                  layout
                  variants={commentItemVariants}
                  initial="initial"
                  animate="animate"
                  exit="exit"
                  className="pl-4 overflow-hidden"
                >
                  <CommentItem
                    comment={reply}
                    videoId={videoId}
                    currentUserId={currentUserId}
                  />
                </motion.div>
              ))}
            </AnimatePresence>

            {hasMoreReplies && (
              <button
                onClick={() => fetchMoreReplies()}
                disabled={isFetchingMoreReplies}
                className="text-xs text-accent/70 hover:text-accent pl-4 font-medium transition-colors"
              >
                {isFetchingMoreReplies ? 'Loading...' : 'Load more replies'}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Main InteractionsBar ───
export default function InteractionsBar({ videoId }) {
  const { isAuthenticated } = useAuthStore();
  const queryClient = useQueryClient();
  const [commentText, setCommentText] = useState('');

  // Fetch the real current user from the server (for delete visibility + optimistic author info)
  const { data: profileData } = useQuery({
    queryKey: ['profile', 'me'],
    queryFn: async () => {
      const res = await api.get('/profile/me');
      return res.data;
    },
    enabled: isAuthenticated,
  });

  const currentUserId = profileData?.userid;

  // Fetch comment count from /getcommentnums (Redis hash, near-real-time, not the stale status cache).
  // refetchInterval keeps it in sync automatically every 45s in the background.
  // After a comment/reply action, we manually invalidate after ~2.5s — TanStack Query then
  // resets the 45s interval timer from that fetch, so the cycle stays consistent.
  const { data: commentCountData } = useQuery({
    queryKey: ['comment-count', videoId],
    queryFn: async () => {
      const res = await api.get(`/getcommentnums?video_id=${videoId}`);
      return res.data;
    },
    enabled: !!videoId,
    staleTime: 0,           // always re-validate on window focus
    refetchInterval: 45 * 1000,  // poll every 45 seconds
    refetchIntervalInBackground: false, // pause polling when tab is not focused
  });

  const commentCount = commentCountData?.comment_counts ?? '—';

  // Fetch top-level comments with cursor pagination
  const {
    data: commentsData,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery({
    queryKey: ['comments', videoId],
    queryFn: async ({ pageParam }) => {
      const p = new URLSearchParams();
      p.append('video_id', videoId);
      if (pageParam?.cursor_time) p.append('cursor_time', pageParam.cursor_time);
      if (pageParam?.cursor_id) p.append('cursor_id', pageParam.cursor_id);
      const res = await api.get(`/comments?${p.toString()}`);
      return res.data;
    },
    getNextPageParam: (lastPage) => {
      if (lastPage.next_cursor_time && lastPage.next_cursor_id) {
        return { cursor_time: lastPage.next_cursor_time, cursor_id: lastPage.next_cursor_id };
      }
      return undefined;
    },
    enabled: !!videoId,
  });

  const allComments = commentsData?.pages.flatMap(p => p.comments || []) || [];

  // ── Post comment mutation — with optimistic update ────────────────────────
  const postCommentMutation = useMutation({
    mutationFn: async (text) => {
      return api.post('/comment', { video_id: videoId, text, parent_id: '' });
    },

    onMutate: async (text) => {
      // Author info is already in the query cache from the profile fetch above
      const author = {
        userId: profileData?.userid || currentUserId || '',
        username: profileData?.username || 'You',
      };
      const optimisticComment = buildOptimisticComment(text, author, null);

      // Cancel any in-flight refetch so it won't race and wipe our optimistic comment
      await queryClient.cancelQueries({ queryKey: ['comments', videoId] });
      const previousComments = queryClient.getQueryData(['comments', videoId]);

      // Instantly prepend the optimistic comment to the top of the list
      queryClient.setQueryData(
        ['comments', videoId],
        (old) => prependToInfiniteCache(old, optimisticComment)
      );

      // Clear the input immediately (standard optimistic UX — restore on error)
      setCommentText('');

      return { previousComments };
    },

    onSuccess: () => {
      toast.success('Comment posted!');
      // Backend writes async via Redis stream. Wait ~2.5s for worker to process,
      // then invalidate so the real comment (with DB id) replaces the optimistic one.
      // The backend's GetComments also bubbles the current user's own comments to the top,
      // so after invalidation the real comment appears at position 0 seamlessly.
      // Invalidating comment-count also resets the 45s polling interval from this point.
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['comments', videoId] });
        queryClient.invalidateQueries({ queryKey: ['comment-count', videoId] });
      }, 2500);
    },

    onError: (err, text, context) => {
      // Rollback: restore previous list state
      if (context?.previousComments !== undefined) {
        queryClient.setQueryData(['comments', videoId], context.previousComments);
      }
      // Restore the comment text so user can retry without retyping
      setCommentText(text);
      toast.error('Failed to post comment. Please try again.');
    },
  });

  const handlePostComment = (e) => {
    e.preventDefault();
    if (!isAuthenticated) return toast.error('Please login first');
    if (!commentText.trim()) return;
    postCommentMutation.mutate(commentText.trim());
  };

  return (
    <div className="mt-6">
      {/* Comments Section */}
      <div className="mt-6">
        <div className="flex items-center gap-2 mb-5">
          <MessageCircle size={18} className="text-white/40" />
          <h3 className="text-base font-bold text-white">
            {commentCount} Comments
          </h3>
        </div>

        {/* Post Comment */}
        <form onSubmit={handlePostComment} className="flex items-center gap-3 mb-8">
          <div className="w-9 h-9 rounded-full bg-white/[0.06] border border-white/10 flex items-center justify-center shrink-0">
            <User size={16} className="text-white/30" />
          </div>
          <input
            type="text"
            placeholder="Add a comment..."
            value={commentText}
            onChange={(e) => setCommentText(e.target.value)}
            className="flex-1 bg-transparent border-b border-white/10 focus:border-white/30 text-white text-sm py-2.5 focus:outline-none transition-colors placeholder:text-white/20"
          />
          <button
            type="submit"
            disabled={postCommentMutation.isPending || !commentText.trim()}
            className="p-2.5 rounded-full bg-accent/20 text-accent hover:bg-accent/30 transition-all disabled:opacity-30 disabled:cursor-not-allowed"
          >
            {postCommentMutation.isPending ? (
              <Loader2 size={16} className="animate-spin" />
            ) : (
              <Send size={16} />
            )}
          </button>
        </form>

        {/* Comments List — animated with framer-motion */}
        <motion.div layout className="flex flex-col gap-6">
          <AnimatePresence mode="popLayout">
            {allComments.map(comment => (
              <motion.div
                key={comment.id}
                layout
                variants={commentItemVariants}
                initial="initial"
                animate="animate"
                exit="exit"
                className="overflow-hidden"
              >
                <CommentItem
                  comment={comment}
                  videoId={videoId}
                  currentUserId={currentUserId}
                />
              </motion.div>
            ))}
          </AnimatePresence>
        </motion.div>

        {/* Load More */}
        {hasNextPage && (
          <button
            onClick={() => fetchNextPage()}
            disabled={isFetchingNextPage}
            className="mt-6 w-full py-3 text-sm text-white/40 hover:text-white/60 font-medium transition-colors border border-white/[0.06] rounded-xl hover:bg-white/[0.03]"
          >
            {isFetchingNextPage ? (
              <span className="flex items-center justify-center gap-2">
                <Loader2 size={14} className="animate-spin" /> Loading...
              </span>
            ) : (
              'Load more comments'
            )}
          </button>
        )}

        {/* Empty State */}
        {allComments.length === 0 && !isFetchingNextPage && (
          <div className="text-center py-10">
            <MessageCircle size={32} className="text-white/10 mx-auto mb-3" />
            <p className="text-white/30 text-sm">
              No comments yet. Be the first to start the discussion!
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
