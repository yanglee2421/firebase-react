import{j as t,z as l,_ as n,A as M,r as m,l as P,o as y}from"./index-pnp19_I-.js";import{a as B,g,s as S,r as R,c as _,b as H}from"./TransitionGroupContext-4nvgwR1C.js";import{S as U}from"./SwitchBase-4hTMVfGy.js";import{c as h}from"./createSvgIcon-RhCEmyU4.js";const E=h(t.jsx("path",{d:"M19 5v14H5V5h14m0-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2z"}),"CheckBoxOutlineBlank"),O=h(t.jsx("path",{d:"M19 3H5c-1.11 0-2 .9-2 2v14c0 1.1.89 2 2 2h14c1.11 0 2-.9 2-2V5c0-1.1-.89-2-2-2zm-9 14l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"}),"CheckBox"),V=h(t.jsx("path",{d:"M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-2 10H7v-2h10v2z"}),"IndeterminateCheckBox");function L(o){return g("MuiCheckbox",o)}const N=B("MuiCheckbox",["root","checked","disabled","indeterminate","colorPrimary","colorSecondary","sizeSmall","sizeMedium"]),u=N,F=["checkedIcon","color","icon","indeterminate","indeterminateIcon","inputProps","size","className"],A=o=>{const{classes:e,indeterminate:c,color:a,size:r}=o,s={root:["root",c&&"indeterminate",`color${l(a)}`,`size${l(r)}`]},d=H(s,L,e);return n({},e,d)},T=S(U,{shouldForwardProp:o=>R(o)||o==="classes",name:"MuiCheckbox",slot:"Root",overridesResolver:(o,e)=>{const{ownerState:c}=o;return[e.root,c.indeterminate&&e.indeterminate,e[`size${l(c.size)}`],c.color!=="default"&&e[`color${l(c.color)}`]]}})(({theme:o,ownerState:e})=>n({color:(o.vars||o).palette.text.secondary},!e.disableRipple&&{"&:hover":{backgroundColor:o.vars?`rgba(${e.color==="default"?o.vars.palette.action.activeChannel:o.vars.palette[e.color].mainChannel} / ${o.vars.palette.action.hoverOpacity})`:M(e.color==="default"?o.palette.action.active:o.palette[e.color].main,o.palette.action.hoverOpacity),"@media (hover: none)":{backgroundColor:"transparent"}}},e.color!=="default"&&{[`&.${u.checked}, &.${u.indeterminate}`]:{color:(o.vars||o).palette[e.color].main},[`&.${u.disabled}`]:{color:(o.vars||o).palette.action.disabled}})),W=t.jsx(O,{}),q=t.jsx(E,{}),w=t.jsx(V,{}),D=m.forwardRef(function(e,c){var a,r;const s=P({props:e,name:"MuiCheckbox"}),{checkedIcon:d=W,color:f="primary",icon:z=q,indeterminate:i=!1,indeterminateIcon:x=w,inputProps:I,size:p="medium",className:$}=s,j=y(s,F),k=i?x:z,C=i?x:d,v=n({},s,{color:f,indeterminate:i,size:p}),b=A(v);return t.jsx(T,n({type:"checkbox",inputProps:n({"data-indeterminate":i},I),icon:m.cloneElement(k,{fontSize:(a=k.props.fontSize)!=null?a:p}),checkedIcon:m.cloneElement(C,{fontSize:(r=C.props.fontSize)!=null?r:p}),ownerState:v,ref:c,className:_(b.root,$)},j,{classes:b}))}),X=D;export{X as M};
