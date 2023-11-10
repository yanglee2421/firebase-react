import{_ as g,r as E,o as pe,q as me,p as xe,j as e,R as p,t as fe,v as ve,w as je,L as ge,b as be}from"./index-3c5663aa.js";import{S as ze}from"./scrollbar-3ec6bfdd.js";import{c as b}from"./createSvgIcon-64d8bfad.js";import{F as Y}from"./FiberManualRecordOutlined-c3047f30.js";import{L as Z}from"./ListItemButton-a52cc8b9.js";import{L as B}from"./ListItemIcon-47bc48e6.js";import{L as W}from"./ListItemText-f91341e2.js";import{a as He,g as Ce,s as U,b as we,c as Me,d as Ee}from"./ButtonBase-b325107b.js";import{g as Q,L as Le}from"./List-d9084187.js";import{u as ye}from"./useTheme-78055a87.js";import{T as Se}from"./Transition-204cc3d3.js";import{I as V}from"./IconButton-4c186eb0.js";import{M as Oe,a as Te}from"./Drawer-199d1efa.js";import{B as R}from"./Box-cc1663ee.js";import{C as Re,S as X}from"./Search-26f3d2d5.js";import{u as Ie}from"./useTranslation-4497ad0d.js";import{M as ee,O as Ve}from"./OutlinedInput-6f381e69.js";import{M as I}from"./MenuItem-068563ad.js";import{u as Fe}from"./use-login-173b7f3d.js";import{B as ke}from"./Badge-eea94f4b.js";import{A as Ae}from"./Avatar-b06ff1d1.js";import{D as De}from"./Divider-1749b3af.js";import{u as Pe,c as Ne,F as $e}from"./index.esm-b92673cb.js";import{B as Be}from"./Modal-2852bca1.js";import{M as We}from"./InputAdornment-d2d019fc.js";import{T as Ue}from"./theme-toggle-a0af67cd.js";import{T as Ge}from"./Toolbar-10e71301.js";import{G as _e}from"./GitHub-5ae95e80.js";import"./ListContext-8af37b8a.js";import"./listItemIconClasses-646ec482.js";import"./listItemTextClasses-9174a24a.js";import"./Typography-21a806b1.js";import"./extendSxProp-22a70640.js";import"./Paper-5ae54d4d.js";import"./ownerWindow-698471fc.js";import"./ownerDocument-613eb639.js";import"./Grow-fb25d800.js";import"./useFormControl-c7418a14.js";import"./TextareaAutosize-52674ff0.js";import"./dividerClasses-e46da641.js";import"./usePreviousProps-1ce8c076.js";function Ke(o){return He("MuiCollapse",o)}Ce("MuiCollapse",["root","horizontal","vertical","entered","hidden","wrapper","wrapperInner"]);const qe=["addEndListener","children","className","collapsedSize","component","easing","in","onEnter","onEntered","onEntering","onExit","onExited","onExiting","orientation","style","timeout","TransitionComponent"],Je=o=>{const{orientation:t,classes:r}=o,i={root:["root",`${t}`],entered:["entered"],hidden:["hidden"],wrapper:["wrapper",`${t}`],wrapperInner:["wrapperInner",`${t}`]};return Ee(i,Ke,r)},Qe=U("div",{name:"MuiCollapse",slot:"Root",overridesResolver:(o,t)=>{const{ownerState:r}=o;return[t.root,t[r.orientation],r.state==="entered"&&t.entered,r.state==="exited"&&!r.in&&r.collapsedSize==="0px"&&t.hidden]}})(({theme:o,ownerState:t})=>g({height:0,overflow:"hidden",transition:o.transitions.create("height")},t.orientation==="horizontal"&&{height:"auto",width:0,transition:o.transitions.create("width")},t.state==="entered"&&g({height:"auto",overflow:"visible"},t.orientation==="horizontal"&&{width:"auto"}),t.state==="exited"&&!t.in&&t.collapsedSize==="0px"&&{visibility:"hidden"})),Xe=U("div",{name:"MuiCollapse",slot:"Wrapper",overridesResolver:(o,t)=>t.wrapper})(({ownerState:o})=>g({display:"flex",width:"100%"},o.orientation==="horizontal"&&{width:"auto",height:"100%"})),Ye=U("div",{name:"MuiCollapse",slot:"WrapperInner",overridesResolver:(o,t)=>t.wrapperInner})(({ownerState:o})=>g({width:"100%"},o.orientation==="horizontal"&&{width:"auto",height:"100%"})),te=E.forwardRef(function(t,r){const i=pe({props:t,name:"MuiCollapse"}),{addEndListener:d,children:a,className:c,collapsedSize:l="0px",component:x,easing:f,in:u,onEnter:j,onEntered:s,onEntering:h,onExit:z,onExited:ne,onExiting:G,orientation:_="vertical",style:P,timeout:C=me.standard,TransitionComponent:oe=Se}=i,re=xe(i,qe),F=g({},i,{orientation:_,collapsedSize:l}),y=Je(F),K=ye(),q=E.useRef(),w=E.useRef(null),N=E.useRef(),k=typeof l=="number"?`${l}px`:l,S=_==="horizontal",O=S?"width":"height";E.useEffect(()=>()=>{clearTimeout(q.current)},[]);const A=E.useRef(null),se=we(r,A),M=n=>v=>{if(n){const H=A.current;v===void 0?n(H):n(H,v)}},$=()=>w.current?w.current[S?"clientWidth":"clientHeight"]:0,ie=M((n,v)=>{w.current&&S&&(w.current.style.position="absolute"),n.style[O]=k,j&&j(n,v)}),ae=M((n,v)=>{const H=$();w.current&&S&&(w.current.style.position="");const{duration:T,easing:D}=Q({style:P,timeout:C,easing:f},{mode:"enter"});if(C==="auto"){const J=K.transitions.getAutoHeightDuration(H);n.style.transitionDuration=`${J}ms`,N.current=J}else n.style.transitionDuration=typeof T=="string"?T:`${T}ms`;n.style[O]=`${H}px`,n.style.transitionTimingFunction=D,h&&h(n,v)}),le=M((n,v)=>{n.style[O]="auto",s&&s(n,v)}),ce=M(n=>{n.style[O]=`${$()}px`,z&&z(n)}),de=M(ne),ue=M(n=>{const v=$(),{duration:H,easing:T}=Q({style:P,timeout:C,easing:f},{mode:"exit"});if(C==="auto"){const D=K.transitions.getAutoHeightDuration(v);n.style.transitionDuration=`${D}ms`,N.current=D}else n.style.transitionDuration=typeof H=="string"?H:`${H}ms`;n.style[O]=k,n.style.transitionTimingFunction=T,G&&G(n)}),he=n=>{C==="auto"&&(q.current=setTimeout(n,N.current||0)),d&&d(A.current,n)};return e.jsx(oe,g({in:u,onEnter:ie,onEntered:le,onEntering:ae,onExit:ce,onExited:de,onExiting:ue,addEndListener:he,nodeRef:A,timeout:C==="auto"?null:C},re,{children:(n,v)=>e.jsx(Qe,g({as:x,className:Me(y.root,c,{entered:y.entered,exited:!u&&k==="0px"&&y.hidden}[n]),style:g({[S?"minWidth":"minHeight"]:k},P),ownerState:g({},F,{state:n}),ref:se},v,{children:e.jsx(Xe,{ownerState:g({},F,{state:n}),className:y.wrapper,ref:w,children:e.jsx(Ye,{ownerState:g({},F,{state:n}),className:y.wrapperInner,children:a})})}))}))});te.muiSupportAuto=!0;const Ze=te,et=b([e.jsx("path",{d:"M19 5v14H5V5h14m0-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2z"},"0"),e.jsx("path",{d:"M14 17H7v-2h7v2zm3-4H7v-2h10v2zm0-4H7V7h10v2z"},"1")],"ArticleOutlined"),tt=b(e.jsx("path",{d:"M13 2.05v3.03c3.39.49 6 3.39 6 6.92 0 .9-.18 1.75-.48 2.54l2.6 1.53c.56-1.24.88-2.62.88-4.07 0-5.18-3.95-9.45-9-9.95zM12 19c-3.87 0-7-3.13-7-7 0-3.53 2.61-6.43 6-6.92V2.05c-5.06.5-9 4.76-9 9.95 0 5.52 4.47 10 9.99 10 3.31 0 6.24-1.61 8.06-4.09l-2.6-1.53C16.17 17.98 14.21 19 12 19z"}),"DataSaverOffOutlined"),nt=b(e.jsx("path",{d:"M10.09 15.59 11.5 17l5-5-5-5-1.41 1.41L12.67 11H3v2h9.67l-2.58 2.59zM19 3H5c-1.11 0-2 .9-2 2v4h2V5h14v14H5v-4H3v4c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2z"}),"ExitToApp"),ot=b(e.jsx("path",{d:"M16 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V8l-5-5zm3 16H5V5h10v4h4v10zM7 17h10v-2H7v2zm5-10H7v2h5V7zm-5 6h10v-2H7v2z"}),"FeedOutlined"),rt=b(e.jsx("path",{d:"M2 17h2v.5H3v1h1v.5H2v1h3v-4H2v1zm1-9h1V4H2v1h1v3zm-1 3h1.8L2 13.1v.9h3v-1H3.2L5 10.9V10H2v1zm5-6v2h14V5H7zm0 14h14v-2H7v2zm0-6h14v-2H7v2z"}),"FormatListNumberedOutlined"),st=b(e.jsx("path",{d:"M20 2H4c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zM8 20H4v-4h4v4zm0-6H4v-4h4v4zm0-6H4V4h4v4zm6 12h-4v-4h4v4zm0-6h-4v-4h4v4zm0-6h-4V4h4v4zm6 12h-4v-4h4v4zm0-6h-4v-4h4v4zm0-6h-4V4h4v4z"}),"GridOnOutlined"),it=b(e.jsx("path",{d:"m12 5.69 5 4.5V18h-2v-6H9v6H7v-7.81l5-4.5M12 3 2 12h3v8h6v-6h2v6h6v-8h3L12 3z"}),"HomeOutlined"),at=b(e.jsx("path",{d:"M8.59 16.59 13.17 12 8.59 7.41 10 6l6 6-6 6-1.41-1.41z"}),"KeyboardArrowRight"),lt=b(e.jsx("path",{d:"M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z"}),"Menu"),ct=b(e.jsx("path",{d:"M13 11.33 18 18H6l5-6.67V6h2m2.96-2H8.04c-.42 0-.65.48-.39.81L9 6.5v4.17L3.2 18.4c-.49.66-.02 1.6.8 1.6h16c.82 0 1.29-.94.8-1.6L15 10.67V6.5l1.35-1.69c.26-.33.03-.81-.39-.81z"}),"ScienceOutlined"),dt=b(e.jsx("path",{d:"m12.87 15.07-2.54-2.51.03-.03c1.74-1.94 2.98-4.17 3.71-6.53H17V4h-7V2H8v2H1v1.99h11.17C11.5 7.92 10.44 9.75 9 11.35 8.07 10.32 7.3 9.19 6.69 8h-2c.73 1.63 1.73 3.17 2.98 4.56l-5.09 5.02L4 19l5-5 3.11 3.11.76-2.04zM18.5 10h-2L12 22h2l1.12-3h4.75L21 22h2l-4.5-12zm-2.62 7 1.62-4.33L19.12 17h-3.24z"}),"Translate");function L(o){const{label:t,icon:r,children:i,...d}=o,[a,c]=p.useState(!1),l=()=>{c(u=>!u)},x=p.useMemo(()=>e.jsx(at,{sx:{transition(u){return u.transitions.create("transform")},transform:a?"rotate(90deg)":""}}),[a]),f=p.useMemo(()=>r||e.jsx(Y,{}),[r]);return e.jsxs(e.Fragment,{children:[e.jsxs(Z,{onClick:l,sx:{backgroundColor(u){return a?u.palette.action.selected:void 0}},...d,children:[e.jsx(B,{children:f}),e.jsx(W,{children:t}),x]}),e.jsx(Ze,{in:a,timeout:"auto",unmountOnExit:!0,children:i})]})}function m(o){const{label:t,icon:r,to:i,end:d,caseSensitive:a,relative:c,...l}=o,x=p.useMemo(()=>r||e.jsx(Y,{}),[r]),f=fe(i,{relative:c}),u=ve(),{navigator:j}=p.useContext(je);let s=u.pathname,h=j.encodeLocation?j.encodeLocation(f).pathname:f.pathname;a||(s=s.toLowerCase(),h=h.toLowerCase());const z=s===h||!d&&s.startsWith(h)&&s.charAt(h.length)==="/";return e.jsxs(Z,{component:ge,to:i,selected:z,...l,children:[e.jsx(B,{children:x}),e.jsx(W,{children:t})]})}function ut(o){const{icon:t,...r}=o,[i,d]=p.useState(!1),a=()=>{d(!1)},c=()=>{d(!0)},l=p.useMemo(()=>t||e.jsx(lt,{}),[t]);return e.jsxs(e.Fragment,{children:[e.jsx(V,{onClick:c,...r,children:l}),e.jsx(Oe,{open:i,onClose:a,anchor:"left",sx:{"& .MuiDrawer-paper":{width:"100%",maxWidth:{sm:320}}},children:e.jsxs(R,{display:"flex",flexDirection:"column",boxSizing:"border-box",height:"100%",children:[e.jsx(R,{p:2,borderBottom:1,borderColor:x=>x.palette.divider,display:"flex",justifyContent:"end",alignItems:"center",children:e.jsx(V,{onClick:a,children:e.jsx(Re,{})})}),e.jsx(R,{flex:1,overflow:"hidden",children:e.jsx(ze,{children:e.jsxs(Le,{component:"nav",disablePadding:!0,children:[e.jsx(m,{icon:e.jsx(it,{}),label:"Home",to:"/"}),e.jsx(L,{icon:e.jsx(et,{}),label:"Pages",children:e.jsx(m,{label:"Not Found",to:"/404"})}),e.jsxs(L,{icon:e.jsx(ot,{}),label:"Form",children:[e.jsx(m,{label:"Input",to:"/input"}),e.jsx(m,{label:"Upload",to:"/upload"}),e.jsx(m,{label:"Autocomplete",to:"autocomplete"})]}),e.jsxs(L,{label:"Table",icon:e.jsx(st,{}),children:[e.jsx(m,{label:"Data Grid",to:"/data-grid"}),e.jsx(m,{label:"Posthog Insights",to:"/posthog-insights"}),e.jsx(m,{label:"Posthog Events",to:"/posthog-events"})]}),e.jsxs(L,{label:"List",icon:e.jsx(rt,{}),children:[e.jsx(m,{label:"Infinite List",to:"/infinite-list"}),e.jsx(m,{label:"Virtualized List",to:"/virtualized-list"})]}),e.jsxs(L,{label:"Lab",icon:e.jsx(ct,{}),children:[e.jsx(m,{label:"Fabric",to:"/fabric"}),e.jsx(m,{label:"Swiper",to:"/swiper"})]}),e.jsx(L,{label:"Charts",icon:e.jsx(tt,{}),children:e.jsx(m,{label:"Charts",to:"/charts"})})]})})})]})})]})}function ht(o){const{...t}=o,[r,i]=p.useState(null),[d,a]=p.useState(!1),c=()=>{a(!1)},l=j=>{i(j.currentTarget),a(!0)},{i18n:x}=Ie(),f=x.language,u=j=>()=>{x.changeLanguage(j),c()};return e.jsxs(e.Fragment,{children:[e.jsx(V,{onClick:l,...t,children:e.jsx(dt,{})}),e.jsxs(ee,{open:d,onClose:c,anchorEl:r,sx:{mt:4},children:[e.jsx(I,{onClick:u("en-US"),selected:f==="en-US",children:"English"}),e.jsx(I,{onClick:u("zh-CN"),selected:f==="zh-CN",children:"简体中文"})]})]})}function pt(){const[o,t]=p.useState(!1),[r,i]=p.useState(null),d=l=>{i(l.currentTarget),t(!0)},a=()=>{i(null),t(!1)},{signOut:c}=Fe();return e.jsxs(e.Fragment,{children:[e.jsx(ke,{onClick:d,variant:"dot",overlap:"circular",anchorOrigin:{vertical:"bottom",horizontal:"right"},sx:l=>({ml:2,cursor:"pointer","& .MuiBadge-badge":{backgroundColor:"#44b700",color:"#44b700",boxShadow:`0 0 0 2px ${l.palette.background.paper}`,"&::after":{position:"absolute",top:0,left:0,width:"100%",height:"100%",borderRadius:"50%",animation:"ripple 1.2s infinite ease-in-out",border:"1px solid currentColor",content:'""'}},"@keyframes ripple":{"0%":{transform:"scale(.8)",opacity:1},"100%":{transform:"scale(2.4)",opacity:0}}}),children:e.jsx(Ae,{src:"https://avatars.githubusercontent.com/u/122474700?v=4",alt:"avator"})}),e.jsxs(ee,{open:o,onClose:a,anchorEl:r,anchorOrigin:{vertical:"bottom",horizontal:"right"},sx:{"& .MuiMenu-paper":{width:230,mt:4}},children:[e.jsx(I,{children:"one"}),e.jsx(I,{children:"one"}),e.jsx(De,{}),e.jsxs(I,{onClick:c,children:[e.jsx(B,{children:e.jsx(nt,{})}),e.jsx(W,{children:"Logout"})]})]})]})}function mt(o){const{...t}=o,[r,i]=p.useState(!1),d=p.useRef(null),a=Pe({defaultValues:{search:""}}),{field:c}=Ne({control:a.control,name:"search"}),l=()=>{i(!1),document.body.style.overflow=""},x=()=>{i(!0),document.body.style.overflow="hidden"},f=a.handleSubmit(s=>{console.log(s),l()}),u=()=>{a.reset(),l()},j=s=>{c.onChange(s.target.value)};return p.useEffect(()=>{if(!r)return;const s=d.current,h=s==null?void 0:s.querySelector(":scope input");h==null||h.focus()},[r,d]),p.useEffect(()=>{const s=new AbortController,{signal:h}=s;return document.addEventListener("keydown",z=>{if(z.ctrlKey&&z.key==="/"){i(!0);return}},{signal:h}),document.addEventListener("keyup",z=>{if(z.key==="Escape"){i(!1);return}},{signal:h}),()=>{s.abort()}},[i]),e.jsxs(e.Fragment,{children:[e.jsx(V,{onClick:x,...t,children:e.jsx(X,{})}),e.jsx(Be,{open:r,sx:{backgroundColor:"rgba(0,0,0,.15)",backdropFilter:"blur(10px)",zIndex(s){return s.zIndex.modal}},children:e.jsx(R,{width:"80%",maxWidth:450,mt:-96,children:e.jsx("form",{onSubmit:f,autoComplete:"off",noValidate:!0,children:e.jsx($e,{...a,children:e.jsx(Ve,{ref:s=>{c.ref(s),Reflect.set(d,"current",s)},value:c.value,onChange:j,onBlur:u,disabled:c.disabled,name:c.name,fullWidth:!0,startAdornment:e.jsx(We,{position:"start",children:e.jsx(X,{})}),sx:{borderRadius:56},placeholder:"Search...",type:"text"})})})})})]})}function xt(){return e.jsx(Te,{position:"static",sx:{boxShadow(o){return o.shadows[1]}},children:e.jsxs(Ge,{children:[e.jsx(ut,{color:"inherit"}),e.jsx(mt,{color:"inherit",sx:{mr:"auto"}}),e.jsx(ht,{color:"inherit"}),e.jsx(Ue,{color:"inherit"}),e.jsx(V,{href:"https://github.com/yanglee2421/react-mui",target:"_blank",color:"inherit",children:e.jsx(_e,{})}),e.jsx(pt,{})]})})}function on(){const o=be();return e.jsxs(e.Fragment,{children:[e.jsx(xt,{}),e.jsx(R,{children:o})]})}export{on as Component};
