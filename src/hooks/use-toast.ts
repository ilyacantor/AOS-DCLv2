export function useToast() {
  return {
    toast: ({ title, description }: any) => {
      console.log(`[Toast] ${title}: ${description}`);
    }
  };
}
