import { create } from 'zustand';

export const useAuthStore = create((set) => ({
  accessToken: null,
  isAuthenticated: false,
  user: null,

  setAccessToken: (token) => set({ accessToken: token, isAuthenticated: !!token }),
  setUser: (userData) => set({ user: userData }),
  logout: () => set({ accessToken: null, isAuthenticated: false, user: null }),
}));

// Provide some simple helper wrappers using our generic api
export const initializeAuth = async (api) => {
    // If there is an existing refresh token in cookies, trying to hit a protected route
    // or refresh endpoint directly can auto-log the user in
    try {
        const res = await api.get('/refresh-token');
        if (res.data.access_token) {
            useAuthStore.getState().setAccessToken(res.data.access_token);
        }
    } catch(err) {
        console.log("No valid session found during initialization");
    }
}
