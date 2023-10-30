import{j as o,_ as u,r as i,k as K,i as E,f as re,o as vt,R as Re}from"./index-bde29093.js";import{C as xt}from"./copy-btn-a3c3d61a.js";import{B as ce}from"./Box-e8adca5a.js";import{g as Z,b as J,s as B,c as M,a as Q,r as gt,d as St,B as Xe,e as Ct,f as ye}from"./ButtonBase-ed76df1a.js";import{c as U}from"./createSvgIcon-5e811805.js";import{T as We}from"./Typography-ad86ba68.js";import{S as wt}from"./SwitchBase-7be1d2f7.js";import{c as yt,u as jt,a as ve}from"./List-5bb61f32.js";import{u as Be}from"./useTheme-ed9d86bd.js";import{C as Rt}from"./Card-470fab78.js";import{C as Bt}from"./CardContent-20eb8022.js";import{u as Tt}from"./useControlled-5361aa27.js";import{u as It}from"./useFormControl-eb816564.js";import{f as Mt,M as kt}from"./Select-188babcd.js";import{G as O}from"./Grid-a3054a04.js";import{T as Pt}from"./theme-toggle-ffd55aec.js";import{u as $t}from"./use-login-e27cd0ed.js";import{d as Ye,o as qe,a as zt}from"./Paper-a5f2f1a8.js";import{K as Et,a as Nt}from"./KeyboardArrowRight-43bbeb42.js";import{S as Ft}from"./Stack-e4b4de42.js";import{B as At}from"./Button-32f3584a.js";import{T as Lt,G as Wt,a as Ht,u as Vt}from"./Twitter-5efea6d4.js";import{M as He}from"./MenuItem-e0c4e3da.js";import"./Drawer-6a53a443.js";import"./Transition-1ad242f7.js";import"./Grow-94595d1f.js";import"./IconButton-1c777146.js";import"./extendSxProp-896ecf05.js";import"./ListContext-babe4a5e.js";import"./TextareaAutosize-cea90000.js";import"./styled-98f83397.js";import"./listItemIconClasses-77496efd.js";import"./dividerClasses-b99b7e78.js";import"./listItemTextClasses-5f73bb33.js";let oe;function Ke(){if(oe)return oe;const e=document.createElement("div"),t=document.createElement("div");return t.style.width="10px",t.style.height="1px",e.appendChild(t),e.dir="rtl",e.style.fontSize="14px",e.style.width="4px",e.style.height="1px",e.style.position="absolute",e.style.top="-1000px",e.style.overflow="scroll",document.body.appendChild(e),oe="reverse",e.scrollLeft>0?oe="default":(e.scrollLeft=1,e.scrollLeft===0&&(oe="negative")),document.body.removeChild(e),oe}function Dt(e,t){const r=e.scrollLeft;if(t!=="rtl")return r;switch(Ke()){case"negative":return e.scrollWidth-e.clientWidth+r;case"reverse":return e.scrollWidth-e.clientWidth-r;default:return r}}const Ot=U(o.jsx("path",{d:"M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z"}),"Person");function Ut(e){return Z("MuiAvatar",e)}J("MuiAvatar",["root","colorDefault","circular","rounded","square","img","fallback"]);const Gt=["alt","children","className","component","imgProps","sizes","src","srcSet","variant"],_t=e=>{const{classes:t,variant:r,colorDefault:n}=e;return Q({root:["root",r,n&&"colorDefault"],img:["img"],fallback:["fallback"]},Ut,t)},Xt=B("div",{name:"MuiAvatar",slot:"Root",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.root,t[r.variant],r.colorDefault&&t.colorDefault]}})(({theme:e,ownerState:t})=>u({position:"relative",display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0,width:40,height:40,fontFamily:e.typography.fontFamily,fontSize:e.typography.pxToRem(20),lineHeight:1,borderRadius:"50%",overflow:"hidden",userSelect:"none"},t.variant==="rounded"&&{borderRadius:(e.vars||e).shape.borderRadius},t.variant==="square"&&{borderRadius:0},t.colorDefault&&u({color:(e.vars||e).palette.background.default},e.vars?{backgroundColor:e.vars.palette.Avatar.defaultBg}:{backgroundColor:e.palette.mode==="light"?e.palette.grey[400]:e.palette.grey[600]}))),Yt=B("img",{name:"MuiAvatar",slot:"Img",overridesResolver:(e,t)=>t.img})({width:"100%",height:"100%",textAlign:"center",objectFit:"cover",color:"transparent",textIndent:1e4}),qt=B(Ot,{name:"MuiAvatar",slot:"Fallback",overridesResolver:(e,t)=>t.fallback})({width:"75%",height:"75%"});function Kt({crossOrigin:e,referrerPolicy:t,src:r,srcSet:n}){const[l,a]=i.useState(!1);return i.useEffect(()=>{if(!r&&!n)return;a(!1);let f=!0;const d=new Image;return d.onload=()=>{f&&a("loaded")},d.onerror=()=>{f&&a("error")},d.crossOrigin=e,d.referrerPolicy=t,d.src=r,n&&(d.srcset=n),()=>{f=!1}},[e,t,r,n]),l}const Zt=i.forwardRef(function(t,r){const n=K({props:t,name:"MuiAvatar"}),{alt:l,children:a,className:f,component:d="div",imgProps:p,sizes:g,src:b,srcSet:v,variant:S="circular"}=n,y=E(n,Gt);let j=null;const R=Kt(u({},p,{src:b,srcSet:v})),x=b||v,T=x&&R!=="error",C=u({},n,{colorDefault:!T,component:d,variant:S}),N=_t(C);return T?j=o.jsx(Yt,u({alt:l,srcSet:v,src:b,sizes:g,ownerState:C,className:N.img},p)):a!=null?j=a:x&&l?j=l[0]:j=o.jsx(qt,{ownerState:C,className:N.fallback}),o.jsx(Xt,u({as:d,ownerState:C,className:M(N.root,f),ref:r},y,{children:j}))}),Jt=Zt;function Qt(e){return Z("MuiFormGroup",e)}J("MuiFormGroup",["root","row","error"]);const eo=["className","row"],to=e=>{const{classes:t,row:r,error:n}=e;return Q({root:["root",r&&"row",n&&"error"]},Qt,t)},oo=B("div",{name:"MuiFormGroup",slot:"Root",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.root,r.row&&t.row]}})(({ownerState:e})=>u({display:"flex",flexDirection:"column",flexWrap:"wrap"},e.row&&{flexDirection:"row"})),ro=i.forwardRef(function(t,r){const n=K({props:t,name:"MuiFormGroup"}),{className:l,row:a=!1}=n,f=E(n,eo),d=It(),p=Mt({props:n,muiFormControl:d,states:["error"]}),g=u({},n,{row:a,error:p.error}),b=to(g);return o.jsx(oo,u({className:M(b.root,l),ownerState:g,ref:r},f))}),no=ro,lo=U(o.jsx("path",{d:"M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8z"}),"RadioButtonUnchecked"),so=U(o.jsx("path",{d:"M8.465 8.465C9.37 7.56 10.62 7 12 7C14.76 7 17 9.24 17 12C17 13.38 16.44 14.63 15.535 15.535C14.63 16.44 13.38 17 12 17C9.24 17 7 14.76 7 12C7 10.62 7.56 9.37 8.465 8.465Z"}),"RadioButtonChecked"),ao=B("span")({position:"relative",display:"flex"}),io=B(lo)({transform:"scale(1)"}),co=B(so)(({theme:e,ownerState:t})=>u({left:0,position:"absolute",transform:"scale(0)",transition:e.transitions.create("transform",{easing:e.transitions.easing.easeIn,duration:e.transitions.duration.shortest})},t.checked&&{transform:"scale(1)",transition:e.transitions.create("transform",{easing:e.transitions.easing.easeOut,duration:e.transitions.duration.shortest})}));function Ze(e){const{checked:t=!1,classes:r={},fontSize:n}=e,l=u({},e,{checked:t});return o.jsxs(ao,{className:r.root,ownerState:l,children:[o.jsx(io,{fontSize:n,className:r.background,ownerState:l}),o.jsx(co,{fontSize:n,className:r.dot,ownerState:l})]})}const uo=i.createContext(void 0),Je=uo;function fo(){return i.useContext(Je)}function po(e){return Z("MuiRadio",e)}const ho=J("MuiRadio",["root","checked","disabled","colorPrimary","colorSecondary","sizeSmall"]),Ve=ho,bo=["checked","checkedIcon","color","icon","name","onChange","size","className"],mo=e=>{const{classes:t,color:r,size:n}=e,l={root:["root",`color${re(r)}`,n!=="medium"&&`size${re(n)}`]};return u({},t,Q(l,po,t))},vo=B(wt,{shouldForwardProp:e=>gt(e)||e==="classes",name:"MuiRadio",slot:"Root",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.root,r.size!=="medium"&&t[`size${re(r.size)}`],t[`color${re(r.color)}`]]}})(({theme:e,ownerState:t})=>u({color:(e.vars||e).palette.text.secondary},!t.disableRipple&&{"&:hover":{backgroundColor:e.vars?`rgba(${t.color==="default"?e.vars.palette.action.activeChannel:e.vars.palette[t.color].mainChannel} / ${e.vars.palette.action.hoverOpacity})`:vt(t.color==="default"?e.palette.action.active:e.palette[t.color].main,e.palette.action.hoverOpacity),"@media (hover: none)":{backgroundColor:"transparent"}}},t.color!=="default"&&{[`&.${Ve.checked}`]:{color:(e.vars||e).palette[t.color].main}},{[`&.${Ve.disabled}`]:{color:(e.vars||e).palette.action.disabled}}));function xo(e,t){return typeof t=="object"&&t!==null?e===t:String(e)===String(t)}const De=o.jsx(Ze,{checked:!0}),Oe=o.jsx(Ze,{}),go=i.forwardRef(function(t,r){var n,l;const a=K({props:t,name:"MuiRadio"}),{checked:f,checkedIcon:d=De,color:p="primary",icon:g=Oe,name:b,onChange:v,size:S="medium",className:y}=a,j=E(a,bo),R=u({},a,{color:p,size:S}),x=mo(R),T=fo();let C=f;const N=yt(v,T&&T.onChange);let P=b;return T&&(typeof C>"u"&&(C=xo(T.value,a.value)),typeof P>"u"&&(P=T.name)),o.jsx(vo,u({type:"radio",icon:i.cloneElement(g,{fontSize:(n=Oe.props.fontSize)!=null?n:S}),checkedIcon:i.cloneElement(d,{fontSize:(l=De.props.fontSize)!=null?l:S}),ownerState:R,classes:x,name:P,checked:C,onChange:N,ref:r,className:M(x.root,y)},j))}),So=go,Co=["actions","children","defaultValue","name","onChange","value"],wo=i.forwardRef(function(t,r){const{actions:n,children:l,defaultValue:a,name:f,onChange:d,value:p}=t,g=E(t,Co),b=i.useRef(null),[v,S]=Tt({controlled:p,default:a,name:"RadioGroup"});i.useImperativeHandle(n,()=>({focus:()=>{let x=b.current.querySelector("input:not(:disabled):checked");x||(x=b.current.querySelector("input:not(:disabled)")),x&&x.focus()}}),[]);const y=St(r,b),j=jt(f),R=i.useMemo(()=>({name:j,onChange(x){S(x.target.value),d&&d(x,x.target.value)},value:v}),[j,d,S,v]);return o.jsx(Je.Provider,{value:R,children:o.jsx(no,u({role:"radiogroup",ref:y},g,{children:l}))})}),yo=wo;function jo(e){return Z("MuiTab",e)}const Ro=J("MuiTab",["root","labelIcon","textColorInherit","textColorPrimary","textColorSecondary","selected","disabled","fullWidth","wrapped","iconWrapper"]),q=Ro,Bo=["className","disabled","disableFocusRipple","fullWidth","icon","iconPosition","indicator","label","onChange","onClick","onFocus","selected","selectionFollowsFocus","textColor","value","wrapped"],To=e=>{const{classes:t,textColor:r,fullWidth:n,wrapped:l,icon:a,label:f,selected:d,disabled:p}=e,g={root:["root",a&&f&&"labelIcon",`textColor${re(r)}`,n&&"fullWidth",l&&"wrapped",d&&"selected",p&&"disabled"],iconWrapper:["iconWrapper"]};return Q(g,jo,t)},Io=B(Xe,{name:"MuiTab",slot:"Root",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.root,r.label&&r.icon&&t.labelIcon,t[`textColor${re(r.textColor)}`],r.fullWidth&&t.fullWidth,r.wrapped&&t.wrapped]}})(({theme:e,ownerState:t})=>u({},e.typography.button,{maxWidth:360,minWidth:90,position:"relative",minHeight:48,flexShrink:0,padding:"12px 16px",overflow:"hidden",whiteSpace:"normal",textAlign:"center"},t.label&&{flexDirection:t.iconPosition==="top"||t.iconPosition==="bottom"?"column":"row"},{lineHeight:1.25},t.icon&&t.label&&{minHeight:72,paddingTop:9,paddingBottom:9,[`& > .${q.iconWrapper}`]:u({},t.iconPosition==="top"&&{marginBottom:6},t.iconPosition==="bottom"&&{marginTop:6},t.iconPosition==="start"&&{marginRight:e.spacing(1)},t.iconPosition==="end"&&{marginLeft:e.spacing(1)})},t.textColor==="inherit"&&{color:"inherit",opacity:.6,[`&.${q.selected}`]:{opacity:1},[`&.${q.disabled}`]:{opacity:(e.vars||e).palette.action.disabledOpacity}},t.textColor==="primary"&&{color:(e.vars||e).palette.text.secondary,[`&.${q.selected}`]:{color:(e.vars||e).palette.primary.main},[`&.${q.disabled}`]:{color:(e.vars||e).palette.text.disabled}},t.textColor==="secondary"&&{color:(e.vars||e).palette.text.secondary,[`&.${q.selected}`]:{color:(e.vars||e).palette.secondary.main},[`&.${q.disabled}`]:{color:(e.vars||e).palette.text.disabled}},t.fullWidth&&{flexShrink:1,flexGrow:1,flexBasis:0,maxWidth:"none"},t.wrapped&&{fontSize:e.typography.pxToRem(12)})),Mo=i.forwardRef(function(t,r){const n=K({props:t,name:"MuiTab"}),{className:l,disabled:a=!1,disableFocusRipple:f=!1,fullWidth:d,icon:p,iconPosition:g="top",indicator:b,label:v,onChange:S,onClick:y,onFocus:j,selected:R,selectionFollowsFocus:x,textColor:T="inherit",value:C,wrapped:N=!1}=n,P=E(n,Bo),ne=u({},n,{disabled:a,disableFocusRipple:f,selected:R,icon:!!p,iconPosition:g,label:!!v,fullWidth:d,textColor:T,wrapped:N}),ee=To(ne),G=p&&v&&i.isValidElement(p)?i.cloneElement(p,{className:M(ee.iconWrapper,p.props.className)}):p,de=F=>{!R&&S&&S(F,C),y&&y(F)},te=F=>{x&&!R&&S&&S(F,C),j&&j(F)};return o.jsxs(Io,u({focusRipple:!f,className:M(ee.root,l),ref:r,role:"tab","aria-selected":R,disabled:a,onClick:de,onFocus:te,ownerState:ne,tabIndex:R?0:-1},P,{children:[g==="top"||g==="start"?o.jsxs(i.Fragment,{children:[G,v]}):o.jsxs(i.Fragment,{children:[v,G]}),b]}))}),H=Mo;function ko(e){return(1+Math.sin(Math.PI*e-Math.PI/2))/2}function Po(e,t,r,n={},l=()=>{}){const{ease:a=ko,duration:f=300}=n;let d=null;const p=t[e];let g=!1;const b=()=>{g=!0},v=S=>{if(g){l(new Error("Animation cancelled"));return}d===null&&(d=S);const y=Math.min(1,(S-d)/f);if(t[e]=a(y)*(r-p)+p,y>=1){requestAnimationFrame(()=>{l(null)});return}requestAnimationFrame(v)};return p===r?(l(new Error("Element already at target position")),b):(requestAnimationFrame(v),b)}const $o=["onChange"],zo={width:99,height:99,position:"absolute",top:-9999,overflow:"scroll"};function Eo(e){const{onChange:t}=e,r=E(e,$o),n=i.useRef(),l=i.useRef(null),a=()=>{n.current=l.current.offsetHeight-l.current.clientHeight};return Ct(()=>{const f=Ye(()=>{const p=n.current;a(),p!==n.current&&t(n.current)}),d=qe(l.current);return d.addEventListener("resize",f),()=>{f.clear(),d.removeEventListener("resize",f)}},[t]),i.useEffect(()=>{a(),t(n.current)},[t]),o.jsx("div",u({style:zo,ref:l},r))}function No(e){return Z("MuiTabScrollButton",e)}const Fo=J("MuiTabScrollButton",["root","vertical","horizontal","disabled"]),Ao=Fo,Lo=["className","slots","slotProps","direction","orientation","disabled"],Wo=e=>{const{classes:t,orientation:r,disabled:n}=e;return Q({root:["root",r,n&&"disabled"]},No,t)},Ho=B(Xe,{name:"MuiTabScrollButton",slot:"Root",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.root,r.orientation&&t[r.orientation]]}})(({ownerState:e})=>u({width:40,flexShrink:0,opacity:.8,[`&.${Ao.disabled}`]:{opacity:0}},e.orientation==="vertical"&&{width:"100%",height:40,"& svg":{transform:`rotate(${e.isRtl?-90:90}deg)`}})),Vo=i.forwardRef(function(t,r){var n,l;const a=K({props:t,name:"MuiTabScrollButton"}),{className:f,slots:d={},slotProps:p={},direction:g}=a,b=E(a,Lo),S=Be().direction==="rtl",y=u({isRtl:S},a),j=Wo(y),R=(n=d.StartScrollButtonIcon)!=null?n:Et,x=(l=d.EndScrollButtonIcon)!=null?l:Nt,T=ve({elementType:R,externalSlotProps:p.startScrollButtonIcon,additionalProps:{fontSize:"small"},ownerState:y}),C=ve({elementType:x,externalSlotProps:p.endScrollButtonIcon,additionalProps:{fontSize:"small"},ownerState:y});return o.jsx(Ho,u({component:"div",className:M(j.root,f),ref:r,role:null,ownerState:y,tabIndex:null},b,{children:g==="left"?o.jsx(R,u({},T)):o.jsx(x,u({},C))}))}),Do=Vo;function Oo(e){return Z("MuiTabs",e)}const Uo=J("MuiTabs",["root","vertical","flexContainer","flexContainerVertical","centered","scroller","fixed","scrollableX","scrollableY","hideScrollbar","scrollButtons","scrollButtonsHideMobile","indicator"]),je=Uo,Go=["aria-label","aria-labelledby","action","centered","children","className","component","allowScrollButtonsMobile","indicatorColor","onChange","orientation","ScrollButtonComponent","scrollButtons","selectionFollowsFocus","slots","slotProps","TabIndicatorProps","TabScrollButtonProps","textColor","value","variant","visibleScrollbar"],Ue=(e,t)=>e===t?e.firstChild:t&&t.nextElementSibling?t.nextElementSibling:e.firstChild,Ge=(e,t)=>e===t?e.lastChild:t&&t.previousElementSibling?t.previousElementSibling:e.lastChild,be=(e,t,r)=>{let n=!1,l=r(e,t);for(;l;){if(l===e.firstChild){if(n)return;n=!0}const a=l.disabled||l.getAttribute("aria-disabled")==="true";if(!l.hasAttribute("tabindex")||a)l=r(e,l);else{l.focus();return}}},_o=e=>{const{vertical:t,fixed:r,hideScrollbar:n,scrollableX:l,scrollableY:a,centered:f,scrollButtonsHideMobile:d,classes:p}=e;return Q({root:["root",t&&"vertical"],scroller:["scroller",r&&"fixed",n&&"hideScrollbar",l&&"scrollableX",a&&"scrollableY"],flexContainer:["flexContainer",t&&"flexContainerVertical",f&&"centered"],indicator:["indicator"],scrollButtons:["scrollButtons",d&&"scrollButtonsHideMobile"],scrollableX:[l&&"scrollableX"],hideScrollbar:[n&&"hideScrollbar"]},Oo,p)},Xo=B("div",{name:"MuiTabs",slot:"Root",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[{[`& .${je.scrollButtons}`]:t.scrollButtons},{[`& .${je.scrollButtons}`]:r.scrollButtonsHideMobile&&t.scrollButtonsHideMobile},t.root,r.vertical&&t.vertical]}})(({ownerState:e,theme:t})=>u({overflow:"hidden",minHeight:48,WebkitOverflowScrolling:"touch",display:"flex"},e.vertical&&{flexDirection:"column"},e.scrollButtonsHideMobile&&{[`& .${je.scrollButtons}`]:{[t.breakpoints.down("sm")]:{display:"none"}}})),Yo=B("div",{name:"MuiTabs",slot:"Scroller",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.scroller,r.fixed&&t.fixed,r.hideScrollbar&&t.hideScrollbar,r.scrollableX&&t.scrollableX,r.scrollableY&&t.scrollableY]}})(({ownerState:e})=>u({position:"relative",display:"inline-block",flex:"1 1 auto",whiteSpace:"nowrap"},e.fixed&&{overflowX:"hidden",width:"100%"},e.hideScrollbar&&{scrollbarWidth:"none","&::-webkit-scrollbar":{display:"none"}},e.scrollableX&&{overflowX:"auto",overflowY:"hidden"},e.scrollableY&&{overflowY:"auto",overflowX:"hidden"})),qo=B("div",{name:"MuiTabs",slot:"FlexContainer",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.flexContainer,r.vertical&&t.flexContainerVertical,r.centered&&t.centered]}})(({ownerState:e})=>u({display:"flex"},e.vertical&&{flexDirection:"column"},e.centered&&{justifyContent:"center"})),Ko=B("span",{name:"MuiTabs",slot:"Indicator",overridesResolver:(e,t)=>t.indicator})(({ownerState:e,theme:t})=>u({position:"absolute",height:2,bottom:0,width:"100%",transition:t.transitions.create()},e.indicatorColor==="primary"&&{backgroundColor:(t.vars||t).palette.primary.main},e.indicatorColor==="secondary"&&{backgroundColor:(t.vars||t).palette.secondary.main},e.vertical&&{height:"100%",width:2,right:0})),Zo=B(Eo)({overflowX:"auto",overflowY:"hidden",scrollbarWidth:"none","&::-webkit-scrollbar":{display:"none"}}),_e={},Jo=i.forwardRef(function(t,r){const n=K({props:t,name:"MuiTabs"}),l=Be(),a=l.direction==="rtl",{"aria-label":f,"aria-labelledby":d,action:p,centered:g=!1,children:b,className:v,component:S="div",allowScrollButtonsMobile:y=!1,indicatorColor:j="primary",onChange:R,orientation:x="horizontal",ScrollButtonComponent:T=Do,scrollButtons:C="auto",selectionFollowsFocus:N,slots:P={},slotProps:ne={},TabIndicatorProps:ee={},TabScrollButtonProps:G={},textColor:de="primary",value:te,variant:F="standard",visibleScrollbar:xe=!1}=n,rt=E(n,Go),$=F==="scrollable",k=x==="vertical",le=k?"scrollTop":"scrollLeft",ue=k?"top":"left",fe=k?"bottom":"right",ge=k?"clientHeight":"clientWidth",se=k?"height":"width",_=u({},n,{component:S,allowScrollButtonsMobile:y,indicatorColor:j,orientation:x,vertical:k,scrollButtons:C,textColor:de,variant:F,visibleScrollbar:xe,fixed:!$,hideScrollbar:$&&!xe,scrollableX:$&&!k,scrollableY:$&&k,centered:g&&!$,scrollButtonsHideMobile:!y}),L=_o(_),nt=ve({elementType:P.StartScrollButtonIcon,externalSlotProps:ne.startScrollButtonIcon,ownerState:_}),lt=ve({elementType:P.EndScrollButtonIcon,externalSlotProps:ne.endScrollButtonIcon,ownerState:_}),[Te,st]=i.useState(!1),[X,Ie]=i.useState(_e),[Me,at]=i.useState(!1),[ke,it]=i.useState(!1),[Pe,ct]=i.useState(!1),[$e,dt]=i.useState({overflow:"hidden",scrollbarWidth:0}),ze=new Map,A=i.useRef(null),Y=i.useRef(null),Ee=()=>{const s=A.current;let c;if(s){const h=s.getBoundingClientRect();c={clientWidth:s.clientWidth,scrollLeft:s.scrollLeft,scrollTop:s.scrollTop,scrollLeftNormalized:Dt(s,l.direction),scrollWidth:s.scrollWidth,top:h.top,bottom:h.bottom,left:h.left,right:h.right}}let m;if(s&&te!==!1){const h=Y.current.children;if(h.length>0){const w=h[ze.get(te)];m=w?w.getBoundingClientRect():null}}return{tabsMeta:c,tabMeta:m}},ae=ye(()=>{const{tabsMeta:s,tabMeta:c}=Ee();let m=0,h;if(k)h="top",c&&s&&(m=c.top-s.top+s.scrollTop);else if(h=a?"right":"left",c&&s){const I=a?s.scrollLeftNormalized+s.clientWidth-s.scrollWidth:s.scrollLeft;m=(a?-1:1)*(c[h]-s[h]+I)}const w={[h]:m,[se]:c?c[se]:0};if(isNaN(X[h])||isNaN(X[se]))Ie(w);else{const I=Math.abs(X[h]-w[h]),z=Math.abs(X[se]-w[se]);(I>=1||z>=1)&&Ie(w)}}),Se=(s,{animation:c=!0}={})=>{c?Po(le,A.current,s,{duration:l.transitions.duration.standard}):A.current[le]=s},Ne=s=>{let c=A.current[le];k?c+=s:(c+=s*(a?-1:1),c*=a&&Ke()==="reverse"?-1:1),Se(c)},Fe=()=>{const s=A.current[ge];let c=0;const m=Array.from(Y.current.children);for(let h=0;h<m.length;h+=1){const w=m[h];if(c+w[ge]>s){h===0&&(c=s);break}c+=w[ge]}return c},ut=()=>{Ne(-1*Fe())},ft=()=>{Ne(Fe())},pt=i.useCallback(s=>{dt({overflow:null,scrollbarWidth:s})},[]),ht=()=>{const s={};s.scrollbarSizeListener=$?o.jsx(Zo,{onChange:pt,className:M(L.scrollableX,L.hideScrollbar)}):null;const m=$&&(C==="auto"&&(Me||ke)||C===!0);return s.scrollButtonStart=m?o.jsx(T,u({slots:{StartScrollButtonIcon:P.StartScrollButtonIcon},slotProps:{startScrollButtonIcon:nt},orientation:x,direction:a?"right":"left",onClick:ut,disabled:!Me},G,{className:M(L.scrollButtons,G.className)})):null,s.scrollButtonEnd=m?o.jsx(T,u({slots:{EndScrollButtonIcon:P.EndScrollButtonIcon},slotProps:{endScrollButtonIcon:lt},orientation:x,direction:a?"left":"right",onClick:ft,disabled:!ke},G,{className:M(L.scrollButtons,G.className)})):null,s},Ae=ye(s=>{const{tabsMeta:c,tabMeta:m}=Ee();if(!(!m||!c)){if(m[ue]<c[ue]){const h=c[le]+(m[ue]-c[ue]);Se(h,{animation:s})}else if(m[fe]>c[fe]){const h=c[le]+(m[fe]-c[fe]);Se(h,{animation:s})}}}),pe=ye(()=>{$&&C!==!1&&ct(!Pe)});i.useEffect(()=>{const s=Ye(()=>{A.current&&ae()});let c;const m=I=>{I.forEach(z=>{z.removedNodes.forEach(ie=>{var W;(W=c)==null||W.unobserve(ie)}),z.addedNodes.forEach(ie=>{var W;(W=c)==null||W.observe(ie)})}),s(),pe()},h=qe(A.current);h.addEventListener("resize",s);let w;return typeof ResizeObserver<"u"&&(c=new ResizeObserver(s),Array.from(Y.current.children).forEach(I=>{c.observe(I)})),typeof MutationObserver<"u"&&(w=new MutationObserver(m),w.observe(Y.current,{childList:!0})),()=>{var I,z;s.clear(),h.removeEventListener("resize",s),(I=w)==null||I.disconnect(),(z=c)==null||z.disconnect()}},[ae,pe]),i.useEffect(()=>{const s=Array.from(Y.current.children),c=s.length;if(typeof IntersectionObserver<"u"&&c>0&&$&&C!==!1){const m=s[0],h=s[c-1],w={root:A.current,threshold:.99},I=we=>{at(!we[0].isIntersecting)},z=new IntersectionObserver(I,w);z.observe(m);const ie=we=>{it(!we[0].isIntersecting)},W=new IntersectionObserver(ie,w);return W.observe(h),()=>{z.disconnect(),W.disconnect()}}},[$,C,Pe,b==null?void 0:b.length]),i.useEffect(()=>{st(!0)},[]),i.useEffect(()=>{ae()}),i.useEffect(()=>{Ae(_e!==X)},[Ae,X]),i.useImperativeHandle(p,()=>({updateIndicator:ae,updateScrollButtons:pe}),[ae,pe]);const Le=o.jsx(Ko,u({},ee,{className:M(L.indicator,ee.className),ownerState:_,style:u({},X,ee.style)}));let he=0;const bt=i.Children.map(b,s=>{if(!i.isValidElement(s))return null;const c=s.props.value===void 0?he:s.props.value;ze.set(c,he);const m=c===te;return he+=1,i.cloneElement(s,u({fullWidth:F==="fullWidth",indicator:m&&!Te&&Le,selected:m,selectionFollowsFocus:N,onChange:R,textColor:de,value:c},he===1&&te===!1&&!s.props.tabIndex?{tabIndex:0}:{}))}),mt=s=>{const c=Y.current,m=zt(c).activeElement;if(m.getAttribute("role")!=="tab")return;let w=x==="horizontal"?"ArrowLeft":"ArrowUp",I=x==="horizontal"?"ArrowRight":"ArrowDown";switch(x==="horizontal"&&a&&(w="ArrowRight",I="ArrowLeft"),s.key){case w:s.preventDefault(),be(c,m,Ge);break;case I:s.preventDefault(),be(c,m,Ue);break;case"Home":s.preventDefault(),be(c,null,Ue);break;case"End":s.preventDefault(),be(c,null,Ge);break}},Ce=ht();return o.jsxs(Xo,u({className:M(L.root,v),ownerState:_,ref:r,as:S},rt,{children:[Ce.scrollButtonStart,Ce.scrollbarSizeListener,o.jsxs(Yo,{className:L.scroller,ownerState:_,style:{overflow:$e.overflow,[k?`margin${a?"Left":"Right"}`:"marginBottom"]:xe?void 0:-$e.scrollbarWidth},ref:A,children:[o.jsx(qo,{"aria-label":f,"aria-labelledby":d,"aria-orientation":x==="vertical"?"vertical":null,className:L.flexContainer,ownerState:_,onKeyDown:mt,ref:Y,role:"tablist",children:bt}),Te&&Le]}),Ce.scrollButtonEnd]}))}),Qo=Jo,er=U(o.jsx("path",{d:"M18.71 19.5c-.83 1.24-1.71 2.45-3.05 2.47-1.34.03-1.77-.79-3.29-.79-1.53 0-2 .77-3.27.82-1.31.05-2.3-1.32-3.14-2.53C4.25 17 2.94 12.45 4.7 9.39c.87-1.52 2.43-2.48 4.12-2.51 1.28-.02 2.5.87 3.29.87.78 0 2.26-1.07 3.81-.91.65.03 2.47.26 3.64 1.98-.09.06-2.17 1.28-2.15 3.81.03 3.02 2.65 4.03 2.68 4.04-.03.07-.42 1.44-1.38 2.83M13 3.5c.73-.83 1.94-1.46 2.94-1.5.13 1.17-.34 2.35-1.04 3.19-.69.85-1.83 1.51-2.95 1.42-.15-1.15.41-2.35 1.05-3.11z"}),"Apple"),tr=U(o.jsx("path",{d:"M5 3h14a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2m13 2h-2.5A3.5 3.5 0 0 0 12 8.5V11h-2v3h2v7h3v-7h3v-3h-3V9a1 1 0 0 1 1-1h2V5z"}),"Facebook"),or=U(o.jsx("path",{d:"M7.8 2h8.4C19.4 2 22 4.6 22 7.8v8.4a5.8 5.8 0 0 1-5.8 5.8H7.8C4.6 22 2 19.4 2 16.2V7.8A5.8 5.8 0 0 1 7.8 2m-.2 2A3.6 3.6 0 0 0 4 7.6v8.8C4 18.39 5.61 20 7.6 20h8.8a3.6 3.6 0 0 0 3.6-3.6V7.6C20 5.61 18.39 4 16.4 4H7.6m9.65 1.5a1.25 1.25 0 0 1 1.25 1.25A1.25 1.25 0 0 1 17.25 8 1.25 1.25 0 0 1 16 6.75a1.25 1.25 0 0 1 1.25-1.25M12 7a5 5 0 0 1 5 5 5 5 0 0 1-5 5 5 5 0 0 1-5-5 5 5 0 0 1 5-5m0 2a3 3 0 0 0-3 3 3 3 0 0 0 3 3 3 3 0 0 0 3-3 3 3 0 0 0-3-3z"}),"Instagram"),rr=U(o.jsx("path",{d:"M2 3h9v9H2V3m9 19H2v-9h9v9M21 3v9h-9V3h9m0 19h-9v-9h9v9Z"}),"Microsoft"),nr=U(o.jsx("path",{d:"M10 15l5.19-3L10 9v6m11.56-7.83c.13.47.22 1.1.28 1.9.07.8.1 1.49.1 2.09L22 12c0 2.19-.16 3.8-.44 4.83-.25.9-.83 1.48-1.73 1.73-.47.13-1.33.22-2.65.28-1.3.07-2.49.1-3.59.1L12 19c-4.19 0-6.8-.16-7.83-.44-.9-.25-1.48-.83-1.73-1.73-.13-.47-.22-1.1-.28-1.9-.07-.8-.1-1.49-.1-2.09L2 12c0-2.19.16-3.8.44-4.83.25-.9.83-1.48 1.73-1.73.47-.13 1.33-.22 2.65-.28 1.3-.07 2.49-.1 3.59-.1L12 5c4.19 0 6.8.16 7.83.44.9.25 1.48.83 1.73 1.73z"}),"YouTube"),Qe=i.createContext(null);function lr(){const[e,t]=i.useState(null);return i.useEffect(()=>{t(`mui-p-${Math.round(Math.random()*1e5)}`)},[]),e}function sr(e){const{children:t,value:r}=e,n=lr(),l=i.useMemo(()=>({idPrefix:n,value:r}),[n,r]);return o.jsx(Qe.Provider,{value:l,children:t})}function et(){return i.useContext(Qe)}function tt(e,t){const{idPrefix:r}=e;return r===null?null:`${e.idPrefix}-P-${t}`}function ot(e,t){const{idPrefix:r}=e;return r===null?null:`${e.idPrefix}-T-${t}`}const ar=["children"],ir=i.forwardRef(function(t,r){const{children:n}=t,l=E(t,ar),a=et();if(a===null)throw new TypeError("No TabContext provided");const f=i.Children.map(n,d=>i.isValidElement(d)?i.cloneElement(d,{"aria-controls":tt(a,d.props.value),id:ot(a,d.props.value)}):null);return o.jsx(Qo,u({},l,{ref:r,value:a.value,children:f}))}),cr=ir;function dr(e){return Z("MuiTabPanel",e)}J("MuiTabPanel",["root"]);const ur=["children","className","value"],fr=e=>{const{classes:t}=e;return Q({root:["root"]},dr,t)},pr=B("div",{name:"MuiTabPanel",slot:"Root",overridesResolver:(e,t)=>t.root})(({theme:e})=>({padding:e.spacing(3)})),hr=i.forwardRef(function(t,r){const n=K({props:t,name:"MuiTabPanel"}),{children:l,className:a,value:f}=n,d=E(n,ur),p=u({},n),g=fr(p),b=et();if(b===null)throw new TypeError("No TabContext provided");const v=tt(b,f),S=ot(b,f);return o.jsx(pr,u({"aria-labelledby":S,className:M(g.root,a),hidden:f!==b.value,id:v,ref:r,role:"tabpanel",ownerState:p},d,{children:f===b.value&&l}))}),V=hr;function br(e){let t=0,r;for(r=0;r<e.length;r+=1)t=e.charCodeAt(r)+((t<<5)-t);let n="#";for(r=0;r<3;r+=1){const l=t>>r*8&255;n+=`00${l.toString(16)}`.slice(-2)}return n}function me(e){const{checked:t,value:r,title:n,desc:l,name:a,...f}=e,d=a.split(" "),p=d.at(0)||"",g=d.at(1)||"",b=(p.at(0)||"")+(g.at(0)||"");return o.jsxs(ce,{component:"label",display:"flex",flexDirection:"column",alignItems:"center",height:"100%",p:2,border:"1px solid",borderColor:v=>t?v.palette.primary.main:v.palette.divider,borderRadius:1,textAlign:"center",sx:{cursor:"pointer"},...f,children:[o.jsx(Jt,{sx:{width:64,height:64,bgcolor:br(a)},children:b.toUpperCase()}),o.jsx(We,{mt:2,fontWeight:500,overflow:"hidden",maxHeight:v=>`calc(${v.typography.body1.lineHeight}em * 1)`,children:n}),o.jsx(We,{variant:"body2",color:"secondary",mt:1,mb:"auto",overflow:"hidden",maxHeight:v=>`calc(${v.typography.body2.lineHeight}em * 4)`,children:l}),o.jsx(So,{value:r})]})}function mr(){const[e,t]=Re.useState("one"),r=(n,l)=>{t(l)};return Be(),o.jsx(Rt,{children:o.jsx(Bt,{children:o.jsx(yo,{value:e,onChange:r,children:o.jsxs(O,{container:!0,spacing:6,children:[o.jsx(O,{item:!0,xs:12,sm:6,md:4,lg:3,xl:2,children:o.jsx(me,{title:"Visual Search",desc:"lorem ",value:"one",checked:e==="one",name:"Kent Dodds"})}),o.jsx(O,{item:!0,xs:12,sm:6,md:4,lg:3,xl:2,children:o.jsx(me,{title:"Visual Search",desc:"lorem ",value:"two",checked:e==="two",name:"Jed Watson"})}),o.jsx(O,{item:!0,xs:12,sm:6,md:4,lg:3,xl:2,children:o.jsx(me,{title:"Visual Search",desc:"lorem ",value:"three",checked:e==="three",name:"Tim Neutkens"})}),o.jsx(O,{item:!0,xs:12,sm:6,md:4,lg:3,xl:2,children:o.jsx(me,{title:"Visual Search",desc:"lorem ",value:"four",checked:e==="four",name:"Yang Lee"})})]})})})})}const vr=B(cr)(({theme:e})=>({"& .MuiTabs-indicator":{display:"none"},"& .Mui-selected":{backgroundColor:e.palette.primary.main,color:`${e.palette.common.white} !important`},"& .MuiTab-root":{minWidth:65,minHeight:38,paddingTop:e.spacing(2.5),paddingBottom:e.spacing(2.5),borderRadius:e.shape.borderRadius,[e.breakpoints.up("sm")]:{minWidth:130}}}));function Jr(){const{signOut:e}=$t(),[t,r]=Re.useState("one"),n=(d,p)=>{r(p)},[l,a]=Re.useState("one"),f=d=>{a(String(d.target.value))};return o.jsxs(o.Fragment,{children:[o.jsxs(O,{container:!0,spacing:3,p:2,children:[o.jsx(O,{item:!0,xs:12,children:o.jsxs(Ft,{direction:"row",spacing:2,children:[o.jsx(At,{onClick:e,variant:"contained",color:"error",children:"sign out"}),o.jsx(xt,{text:"2583169032"}),o.jsx(Pt,{})]})}),o.jsx(O,{item:!0,xs:12,children:o.jsx(mr,{})})]}),o.jsxs(sr,{value:t,children:[o.jsxs(ce,{display:"flex",gap:5,p:2,children:[o.jsx(ce,{flex:1,overflow:"hidden",display:"flex",alignItems:"center",children:o.jsxs(vr,{onChange:n,variant:"scrollable",scrollButtons:"auto",sx:{alignItems:"center"},children:[o.jsx(H,{value:"one",label:o.jsx(D,{icon:o.jsx(tr,{}),children:"one"})}),o.jsx(H,{value:"two",label:o.jsx(D,{icon:o.jsx(Lt,{}),children:"two"})}),o.jsx(H,{value:"three",label:o.jsx(D,{icon:o.jsx(Wt,{}),children:"three"})}),o.jsx(H,{value:"four",label:o.jsx(D,{icon:o.jsx(Ht,{}),children:"four"})}),o.jsx(H,{value:"five",label:o.jsx(D,{icon:o.jsx(rr,{}),children:"five"})}),o.jsx(H,{value:"six",label:o.jsx(D,{icon:o.jsx(er,{}),children:"six"})}),o.jsx(H,{value:"seven",label:o.jsx(D,{icon:o.jsx(nr,{}),children:"seven"})}),o.jsx(H,{value:"eight",label:o.jsx(D,{icon:o.jsx(or,{}),children:"eight"})})]})}),o.jsx(ce,{display:"flex",alignItems:"center",children:o.jsxs(kt,{value:l,onChange:f,size:"small",children:[o.jsx(He,{value:"one",children:"woolworlds one"}),o.jsx(He,{value:"two",children:"woolworlds two"})]})})]}),o.jsx(V,{value:"one",children:"one"}),o.jsx(V,{value:"two",children:"two"}),o.jsx(V,{value:"three",children:"three"}),o.jsx(V,{value:"four",children:"four"}),o.jsx(V,{value:"five",children:"five"}),o.jsx(V,{value:"six",children:"six"}),o.jsx(V,{value:"seven",children:"seven"}),o.jsx(V,{value:"eight",children:"eight"})]})]})}function D(e){const{children:t,icon:r}=e,n=Vt(l=>l.breakpoints.down("sm"));return o.jsxs(ce,{display:"flex",alignItems:"center",sx:{"& svg":{mr:n?0:2}},children:[r,n||t]})}export{Jr as Component};
