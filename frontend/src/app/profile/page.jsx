'use client';
import { useState, useEffect, useRef } from 'react';
import { api } from '@/lib/api';
import { useAuthStore } from '@/lib/store';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { useRouter } from 'next/navigation';
import { User, Camera, Save, ArrowLeft, FileVideo } from 'lucide-react';
import Link from 'next/link';

export default function ProfilePage() {
  const { isAuthenticated } = useAuthStore();
  const router = useRouter();
  const queryClient = useQueryClient();
  const fileInputRef = useRef(null);

  const [formData, setFormData] = useState({ username: '', email: '', dob: '', firstname: '', lastname: '', bio: '' });

  useEffect(() => {
    if (!isAuthenticated) router.push('/login');
  }, [isAuthenticated, router]);

  // Fetch current user details
  const { data: profile, isLoading } = useQuery({
    queryKey: ['profile', 'me'],
    queryFn: async () => {
      const res = await api.get('/profile/me');
      return res.data;
    },
    enabled: isAuthenticated
  });

  // Hydrate local form state when data comes in
  useEffect(() => {
    if (profile) {
      setFormData({
        username: profile.username || '',
        email: profile.email || '',
        dob: profile.dob ? profile.dob.split('T')[0] : '',
        firstname: profile.firstname || '',
        lastname: profile.lastname || '',
        bio: profile.bio || ''
      });
    }
  }, [profile]);

  const updateProfileMutation = useMutation({
    mutationFn: async (data) => api.put('/profile/details', data),
    onSuccess: (res) => {
      // If username changed, backend re-issues tokens — store the new access token
      if (res.data.access_token) {
        useAuthStore.getState().setAccessToken(res.data.access_token);
      }
      toast.success("Profile securely updated!");
      queryClient.invalidateQueries({ queryKey: ['profile', 'me'] });
    },
    onError: (err) => toast.error(err.response?.data?.error || "Update failed")
  });

  const uploadPictureMutation = useMutation({
    mutationFn: async (file) => {
      const fnData = new FormData();
      fnData.append('profile_pic', file);
      return api.put('/profile/picture', fnData, { headers: { 'Content-Type': 'multipart/form-data' }});
    },
    onSuccess: () => {
      toast.success("Profile picture updated!");
      queryClient.invalidateQueries({ queryKey: ['profile', 'me'] });
    },
    onError: (err) => toast.error(err.response?.data?.error || "Upload failed")
  });

  const handleUpdateProfile = (e) => {
    e.preventDefault();
    const payload = {};
    if (formData.username && formData.username !== profile.username) payload.username = formData.username;
    if (formData.email && formData.email !== profile.email) payload.email = formData.email;
    if (formData.dob) payload.dob = `${formData.dob}T00:00:00Z`;
    if (formData.firstname !== (profile.firstname || '')) payload.firstname = formData.firstname;
    if (formData.lastname !== (profile.lastname || '')) payload.lastname = formData.lastname;
    if (formData.bio !== (profile.bio || '')) payload.bio = formData.bio;

    if (Object.keys(payload).length > 0) {
      updateProfileMutation.mutate(payload);
    } else {
      toast("No changes detected.", { icon: 'ℹ️' });
    }
  };

  const handleFileChange = (e) => {
    if (e.target.files && e.target.files[0]) {
       uploadPictureMutation.mutate(e.target.files[0]);
    }
  };

  if (!isAuthenticated) return null;

  return (
    <div className="w-full max-w-2xl mx-auto flex flex-col items-center justify-center min-h-[80vh] px-4 py-8 relative">
      
      {/* ── Background glow ── */}
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[500px] h-[500px] bg-accent/10 rounded-full blur-[100px] -z-10 pointer-events-none"></div>

      <div className="w-full flex items-center justify-between mb-8">
         <Link href="/dashboard" className="flex items-center gap-2 text-white/50 hover:text-white transition-colors text-sm font-medium">
            <ArrowLeft size={16} /> Back to Dashboard
         </Link>
      </div>

      <div 
        className="w-full border rounded-3xl p-8 relative flex flex-col z-10"
        style={{
          background: 'rgba(255,255,255,0.03)',
          backdropFilter: 'blur(24px)',
          borderColor: 'rgba(255,255,255,0.08)',
          boxShadow: '0 0 0 1px rgba(255,255,255,0.04) inset, 0 25px 50px rgba(0,0,0,0.5)'
        }}
      >
        <div className="absolute -top-px left-1/2 -translate-x-1/2 w-1/2 h-px bg-gradient-to-r from-transparent via-white/30 to-transparent rounded-full" />

        <div className="flex flex-col items-center mb-8 border-b border-white/10 pb-8 relative">
           
           <div className="relative group">
             <div className="w-28 h-28 bg-black/40 rounded-full flex justify-center items-center overflow-hidden border border-white/20 shadow-inner transition-all group-hover:border-accent">
                 <User size={40} className="text-white/30" />
                 {/* Realistically show downloaded pic here if returned by backend */}
             </div>
             
             <button 
                onClick={() => fileInputRef.current?.click()}
                disabled={uploadPictureMutation.isPending}
                className="absolute bottom-0 right-0 bg-accent hover:bg-accent-hover p-2.5 rounded-full text-white shadow-[0_4px_14px_0_rgba(255,0,0,0.4)] transition-all hover:-translate-y-0.5"
             >
                <Camera size={16} />
             </button>
           </div>
           
           <input type="file" className="hidden" ref={fileInputRef} onChange={handleFileChange} accept="image/*" />
           
           <h1 className="text-2xl font-black text-white mt-4 drop-shadow-md tracking-tight">Your Identity</h1>
           <p className="text-sm text-white/40 font-light mt-1">Manage your secure platform details</p>

           {/* Videos uploaded stat */}
           <div className="flex items-center gap-2 mt-4 px-4 py-2 rounded-xl bg-white/5 border border-white/8">
             <FileVideo size={14} className="text-accent/70" />
             <span className="text-sm font-bold text-white/70">{profile?.videos_uploaded ?? 0}</span>
             <span className="text-[10px] text-white/40 uppercase tracking-wider font-semibold">Videos Uploaded</span>
           </div>
        </div>

        {isLoading ? (
           <div className="flex justify-center py-12">
             <div className="animate-spin w-8 h-8 border-2 border-accent border-t-transparent rounded-full"></div>
           </div>
        ) : (
           <form onSubmit={handleUpdateProfile} className="flex flex-col gap-5 w-full max-w-md mx-auto">
              <div className="flex gap-3">
                <div className="flex-1">
                  <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">First Name</label>
                  <input 
                    type="text" 
                    value={formData.firstname} 
                    onChange={e => setFormData({...formData, firstname: e.target.value})} 
                    placeholder="First name"
                    className="w-full bg-black/40 px-5 py-3.5 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all shadow-inner placeholder:text-white/20"
                  />
                </div>
                <div className="flex-1">
                  <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">Last Name</label>
                  <input 
                    type="text" 
                    value={formData.lastname} 
                    onChange={e => setFormData({...formData, lastname: e.target.value})} 
                    placeholder="Last name"
                    className="w-full bg-black/40 px-5 py-3.5 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all shadow-inner placeholder:text-white/20"
                  />
                </div>
              </div>

              <div>
                <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">Username</label>
                <input 
                  type="text" 
                  value={formData.username} 
                  onChange={e => setFormData({...formData, username: e.target.value})} 
                  className="w-full bg-black/40 px-5 py-3.5 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all font-mono shadow-inner"
                  required
                />
              </div>

              <div>
                <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">Email Address</label>
                <input 
                  type="email" 
                  value={formData.email} 
                  onChange={e => setFormData({...formData, email: e.target.value})} 
                  className="w-full bg-black/40 px-5 py-3.5 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all font-mono shadow-inner"
                  required
                />
              </div>

              <div>
                <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">Date of Birth</label>
                <input 
                  type="date" 
                  value={formData.dob} 
                  onChange={e => setFormData({...formData, dob: e.target.value})} 
                  className="w-full bg-black/40 px-5 py-3.5 rounded-2xl border border-white/10 text-white/60 text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all font-mono shadow-inner uppercase"
                  style={{ colorScheme: 'dark' }}
                />
              </div>

              <div>
                <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">Bio</label>
                <textarea 
                  value={formData.bio} 
                  onChange={e => setFormData({...formData, bio: e.target.value})} 
                  placeholder="Tell the world about yourself..."
                  rows={3}
                  className="w-full bg-black/40 px-5 py-3.5 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all shadow-inner resize-none placeholder:text-white/20"
                />
              </div>
              
              <button 
                type="submit" 
                disabled={updateProfileMutation.isPending} 
                className="mt-6 flex items-center justify-center gap-2 px-6 py-4 rounded-2xl font-semibold text-sm text-white transition-all hover:-translate-y-0.5 active:translate-y-0 disabled:opacity-50 disabled:hover:translate-y-0"
                style={{ background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)', boxShadow: '0 4px 20px rgba(255,0,0,0.4)' }}
              >
                 {updateProfileMutation.isPending ? (
                   <span className="animate-spin h-5 w-5 border-2 border-white border-t-transparent rounded-full"></span>
                 ) : (
                   <><Save size={16} /> Save Changes</>
                 )}
              </button>
           </form>
        )}
      </div>
    </div>
  );
}

