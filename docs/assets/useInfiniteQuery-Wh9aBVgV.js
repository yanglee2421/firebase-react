import{Q as g,u as d}from"./useBaseQuery-6MAQ8PXg.js";import{O as f,P as v,Q as b}from"./index-pnp19_I-.js";var x=class extends g{constructor(e,t){super(e,t)}bindMethods(){super.bindMethods(),this.fetchNextPage=this.fetchNextPage.bind(this),this.fetchPreviousPage=this.fetchPreviousPage.bind(this)}setOptions(e,t){super.setOptions({...e,behavior:f()},t)}getOptimisticResult(e){return e.behavior=f(),super.getOptimisticResult(e)}fetchNextPage(e){return this.fetch({...e,meta:{fetchMore:{direction:"forward"}}})}fetchPreviousPage(e){return this.fetch({...e,meta:{fetchMore:{direction:"backward"}}})}createResult(e,t){var h,u,n,o;const{state:s}=e,r=super.createResult(e,t),{isFetching:i,isRefetching:P}=r,a=i&&((u=(h=s.fetchMeta)==null?void 0:h.fetchMore)==null?void 0:u.direction)==="forward",c=i&&((o=(n=s.fetchMeta)==null?void 0:n.fetchMore)==null?void 0:o.direction)==="backward";return{...r,fetchNextPage:this.fetchNextPage,fetchPreviousPage:this.fetchPreviousPage,hasNextPage:v(t,s.data),hasPreviousPage:b(t,s.data),isFetchingNextPage:a,isFetchingPreviousPage:c,isRefetching:P&&!a&&!c}}};function M(e,t){return d(e,x,t)}export{M as u};
