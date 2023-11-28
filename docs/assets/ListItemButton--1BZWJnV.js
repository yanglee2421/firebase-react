import{_ as u,A as d,r as p,l as F,o as V,j as y}from"./index-pnp19_I-.js";import{a as j,g as G,s as N,r as M,d as P,u as E,c as I,b as S}from"./TransitionGroupContext-4nvgwR1C.js";import{L as $}from"./ListContext-1957hfWH.js";import{B as U}from"./ButtonBase-M6bKPT5t.js";function _(t){return G("MuiListItemButton",t)}const w=j("MuiListItemButton",["root","focusVisible","dense","alignItemsFlexStart","disabled","divider","gutters","selected"]),n=w,T=["alignItems","autoFocus","component","children","dense","disableGutters","divider","focusVisibleClassName","selected","className"],A=(t,e)=>{const{ownerState:s}=t;return[e.root,s.dense&&e.dense,s.alignItems==="flex-start"&&e.alignItemsFlexStart,s.divider&&e.divider,!s.disableGutters&&e.gutters]},D=t=>{const{alignItems:e,classes:s,dense:a,disabled:o,disableGutters:l,divider:c,selected:f}=t,i=S({root:["root",a&&"dense",!l&&"gutters",c&&"divider",o&&"disabled",e==="flex-start"&&"alignItemsFlexStart",f&&"selected"]},_,s);return u({},s,i)},W=N(U,{shouldForwardProp:t=>M(t)||t==="classes",name:"MuiListItemButton",slot:"Root",overridesResolver:A})(({theme:t,ownerState:e})=>u({display:"flex",flexGrow:1,justifyContent:"flex-start",alignItems:"center",position:"relative",textDecoration:"none",minWidth:0,boxSizing:"border-box",textAlign:"left",paddingTop:8,paddingBottom:8,transition:t.transitions.create("background-color",{duration:t.transitions.duration.shortest}),"&:hover":{textDecoration:"none",backgroundColor:(t.vars||t).palette.action.hover,"@media (hover: none)":{backgroundColor:"transparent"}},[`&.${n.selected}`]:{backgroundColor:t.vars?`rgba(${t.vars.palette.primary.mainChannel} / ${t.vars.palette.action.selectedOpacity})`:d(t.palette.primary.main,t.palette.action.selectedOpacity),[`&.${n.focusVisible}`]:{backgroundColor:t.vars?`rgba(${t.vars.palette.primary.mainChannel} / calc(${t.vars.palette.action.selectedOpacity} + ${t.vars.palette.action.focusOpacity}))`:d(t.palette.primary.main,t.palette.action.selectedOpacity+t.palette.action.focusOpacity)}},[`&.${n.selected}:hover`]:{backgroundColor:t.vars?`rgba(${t.vars.palette.primary.mainChannel} / calc(${t.vars.palette.action.selectedOpacity} + ${t.vars.palette.action.hoverOpacity}))`:d(t.palette.primary.main,t.palette.action.selectedOpacity+t.palette.action.hoverOpacity),"@media (hover: none)":{backgroundColor:t.vars?`rgba(${t.vars.palette.primary.mainChannel} / ${t.vars.palette.action.selectedOpacity})`:d(t.palette.primary.main,t.palette.action.selectedOpacity)}},[`&.${n.focusVisible}`]:{backgroundColor:(t.vars||t).palette.action.focus},[`&.${n.disabled}`]:{opacity:(t.vars||t).palette.action.disabledOpacity}},e.divider&&{borderBottom:`1px solid ${(t.vars||t).palette.divider}`,backgroundClip:"padding-box"},e.alignItems==="flex-start"&&{alignItems:"flex-start"},!e.disableGutters&&{paddingLeft:16,paddingRight:16},e.dense&&{paddingTop:4,paddingBottom:4})),z=p.forwardRef(function(e,s){const a=F({props:e,name:"MuiListItemButton"}),{alignItems:o="center",autoFocus:l=!1,component:c="div",children:f,dense:v=!1,disableGutters:i=!1,divider:B=!1,focusVisibleClassName:L,selected:O=!1,className:k}=a,r=V(a,T),m=p.useContext($),x=p.useMemo(()=>({dense:v||m.dense||!1,alignItems:o,disableGutters:i}),[o,m.dense,v,i]),g=p.useRef(null);P(()=>{l&&g.current&&g.current.focus()},[l]);const C=u({},a,{alignItems:o,dense:x.dense,disableGutters:i,divider:B,selected:O}),b=D(C),R=E(g,s);return y.jsx($.Provider,{value:x,children:y.jsx(W,u({ref:R,href:r.href||r.to,component:(r.href||r.to)&&c==="div"?"button":c,focusVisibleClassName:I(b.focusVisible,L),ownerState:C,className:I(b.root,k)},r,{classes:b,children:f}))})}),Q=z;export{Q as L,n as l};
