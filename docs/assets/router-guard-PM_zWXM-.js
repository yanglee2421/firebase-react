import{u as w,R as E,c as M,g as V,a as q,n as A,b as D,d as j,j as L,N as x}from"./index-pnp19_I-.js";import{u as N}from"./use-login-jFQtDUkP.js";import{u as W}from"./useQuery-6NOfNXCV.js";import"./useBaseQuery-6MAQ8PXg.js";import"./utils-1RRThafF.js";function z(){const i=w();return E.useEffect(()=>{const u=i[i.length-1],t=Reflect.get(Object(u==null?void 0:u.handle),"title");if(t&&typeof t=="string"){document.title=t;return}},[i])}var k={exports:{}};/* NProgress, (c) 2013, 2014 Rico Sta. Cruz - http://ricostacruz.com/nprogress
 * @license MIT */(function(i,u){(function(t,s){i.exports=s()})(M,function(){var t={};t.version="0.2.0";var s=t.settings={minimum:.08,easing:"ease",positionUsing:"",speed:200,trickle:!0,trickleRate:.02,trickleSpeed:800,showSpinner:!0,barSelector:'[role="bar"]',spinnerSelector:'[role="spinner"]',parent:"body",template:'<div class="bar" role="bar"><div class="peg"></div></div><div class="spinner" role="spinner"><div class="spinner-icon"></div></div>'};t.configure=function(e){var r,n;for(r in e)n=e[r],n!==void 0&&e.hasOwnProperty(r)&&(s[r]=n);return this},t.status=null,t.set=function(e){var r=t.isStarted();e=d(e,s.minimum,1),t.status=e===1?null:e;var n=t.render(!r),a=n.querySelector(s.barSelector),l=s.speed,v=s.easing;return n.offsetWidth,b(function(o){s.positionUsing===""&&(s.positionUsing=t.getPositioningCSS()),c(a,p(e,l,v)),e===1?(c(n,{transition:"none",opacity:1}),n.offsetWidth,setTimeout(function(){c(n,{transition:"all "+l+"ms linear",opacity:0}),setTimeout(function(){t.remove(),o()},l)},l)):setTimeout(o,l)}),this},t.isStarted=function(){return typeof t.status=="number"},t.start=function(){t.status||t.set(0);var e=function(){setTimeout(function(){t.status&&(t.trickle(),e())},s.trickleSpeed)};return s.trickle&&e(),this},t.done=function(e){return!e&&!t.status?this:t.inc(.3+.5*Math.random()).set(1)},t.inc=function(e){var r=t.status;return r?(typeof e!="number"&&(e=(1-r)*d(Math.random()*r,.1,.95)),r=d(r+e,0,.994),t.set(r)):t.start()},t.trickle=function(){return t.inc(Math.random()*s.trickleRate)},function(){var e=0,r=0;t.promise=function(n){return!n||n.state()==="resolved"?this:(r===0&&t.start(),e++,r++,n.always(function(){r--,r===0?(e=0,t.done()):t.set((e-r)/e)}),this)}}(),t.render=function(e){if(t.isRendered())return document.getElementById("nprogress");_(document.documentElement,"nprogress-busy");var r=document.createElement("div");r.id="nprogress",r.innerHTML=s.template;var n=r.querySelector(s.barSelector),a=e?"-100":g(t.status||0),l=document.querySelector(s.parent),v;return c(n,{transition:"all 0 linear",transform:"translate3d("+a+"%,0,0)"}),s.showSpinner||(v=r.querySelector(s.spinnerSelector),v&&I(v)),l!=document.body&&_(l,"nprogress-custom-parent"),l.appendChild(r),r},t.remove=function(){U(document.documentElement,"nprogress-busy"),U(document.querySelector(s.parent),"nprogress-custom-parent");var e=document.getElementById("nprogress");e&&I(e)},t.isRendered=function(){return!!document.getElementById("nprogress")},t.getPositioningCSS=function(){var e=document.body.style,r="WebkitTransform"in e?"Webkit":"MozTransform"in e?"Moz":"msTransform"in e?"ms":"OTransform"in e?"O":"";return r+"Perspective"in e?"translate3d":r+"Transform"in e?"translate":"margin"};function d(e,r,n){return e<r?r:e>n?n:e}function g(e){return(-1+e)*100}function p(e,r,n){var a;return s.positionUsing==="translate3d"?a={transform:"translate3d("+g(e)+"%,0,0)"}:s.positionUsing==="translate"?a={transform:"translate("+g(e)+"%,0)"}:a={"margin-left":g(e)+"%"},a.transition="all "+r+"ms "+n,a}var b=function(){var e=[];function r(){var n=e.shift();n&&n(r)}return function(n){e.push(n),e.length==1&&r()}}(),c=function(){var e=["Webkit","O","Moz","ms"],r={};function n(o){return o.replace(/^-ms-/,"ms-").replace(/-([\da-z])/gi,function(f,m){return m.toUpperCase()})}function a(o){var f=document.body.style;if(o in f)return o;for(var m=e.length,y=o.charAt(0).toUpperCase()+o.slice(1),h;m--;)if(h=e[m]+y,h in f)return h;return o}function l(o){return o=n(o),r[o]||(r[o]=a(o))}function v(o,f,m){f=l(f),o.style[f]=m}return function(o,f){var m=arguments,y,h;if(m.length==2)for(y in f)h=f[y],h!==void 0&&f.hasOwnProperty(y)&&v(o,y,h);else v(o,m[1],m[2])}}();function R(e,r){var n=typeof e=="string"?e:T(e);return n.indexOf(" "+r+" ")>=0}function _(e,r){var n=T(e),a=n+r;R(n,r)||(e.className=a.substring(1))}function U(e,r){var n=T(e),a;R(e,r)&&(a=n.replace(" "+r+" "," "),e.className=a.substring(1,a.length-1))}function T(e){return(" "+(e.className||"")+" ").replace(/\s+/gi," ")}function I(e){e&&e.parentNode&&e.parentNode.removeChild(e)}return t})})(k);var B=k.exports;const C=V(B);function G(){const i=w();return E.useEffect(()=>(C.done(),()=>{C.start()}),[i])}function H(i){return q({url:"/usr",...i})}function K(){const{usr:i,updateUsr:u,signOut:t}=N(),{error:s,data:d}=W({enabled:!!i,queryKey:["usr_get"],queryFn({signal:g}){return H({signal:g,params:i})},initialData(){if(i)return i},initialDataUpdatedAt(){return i==null?void 0:i.loginAt},refetchInterval:1e3*60*30,retry:2,retryDelay:1e3*2});E.useEffect(()=>{d&&u(d)},[u,d]),E.useEffect(()=>{s&&(t(),A.error(s.message))},[t,s])}const S=new Set;S.add("login");S.add("list");S.add("forgot-passwd");S.add("register");S.add("privacy-policy");const Y=S.has.bind(S);var O={VITE_BASE_URL:"/dev",VITE_QUERY_PERSISTER_KEY:"react-mui-query-persister",VITE_REDUX_PERSISTER_KEY:"react-mui-redux-persister",VITE_SSO_ORIGIN:"http://localhost:5500",VITE_POSTHOG_URL:"https://data-warpdriven.warpdriven.ai/api",VITE_POSTHOG_APIKEY:"phx_m56yfnzSJ10UqrhEoVSjeewiDHzmzjn7LOqprQ3WSZn",BASE_URL:"./",MODE:"production",DEV:!1,PROD:!0,SSR:!1};function $(){z(),G(),K();const i=D(),u=w(),[t]=j(),{usr:s,signIn:d}=N(),g=E.useMemo(()=>{const p=u[u.length-1];if(!p)return console.error("Invalid nextRoute"),null;if(p.id==="login"){const e=t.get("returnURL")||"/";return s?L.jsx(x,{to:e,replace:!0}):i}if(Y(p.id)||s)return i;const R=new URLSearchParams;R.set("returnURL",p.pathname);const _=R.toString(),I={pathname:"/login",search:p.id==="home"?void 0:_};return L.jsx(x,{to:I,replace:!0})},[u,t,i,s]);return E.useEffect(()=>{const p=new AbortController;return window.addEventListener("message",b=>{if(P.has(b.origin))return;const c=JSON.parse(b.data);c.type==="sso-login"&&d({email:c.email,role:c.role,loginAt:c.loginAt},c.rememberMe)},{signal:p.signal}),()=>{p.abort()}},[d]),L.jsx(L.Fragment,{children:g})}const P=new Set;P.add(O.VITE_REACT_ANTD_URL);P.add(O.VITE_REACT_MUI_URL);P.add(O.VITE_VUE_ELE_URL);export{$ as Component};
