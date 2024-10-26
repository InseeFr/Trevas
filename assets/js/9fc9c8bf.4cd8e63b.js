"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[9319],{92287:(e,n,r)=>{r.r(n),r.d(n,{assets:()=>d,contentTitle:()=>i,default:()=>h,frontMatter:()=>l,metadata:()=>c,toc:()=>u});var t=r(74848),a=r(28453),s=r(86025),o=r(50116);const l={slug:"/trevas-vtl-21",title:"Trevas - VTL 2.1",authors:["nicolas"],tags:["Trevas","VTL 2.1"]},i=void 0,c={permalink:"/Trevas/blog/trevas-vtl-21",source:"@site/blog/2024-10-09-trevas-vtl-21.mdx",title:"Trevas - VTL 2.1",description:"Trevas 1.7.0 upgrade to version 2.1 of VTL.",date:"2024-10-09T00:00:00.000Z",tags:[{inline:!0,label:"Trevas",permalink:"/Trevas/blog/tags/trevas"},{inline:!0,label:"VTL 2.1",permalink:"/Trevas/blog/tags/vtl-2-1"}],readingTime:.51,hasTruncateMarker:!1,authors:[{name:"Nicolas Laval",link:"https://github.com/NicoLaval",title:"Making Sense - Developer",image:"/img/profile_pic_nicolas_laval.jpg",key:"nicolas",page:null}],frontMatter:{slug:"/trevas-vtl-21",title:"Trevas - VTL 2.1",authors:["nicolas"],tags:["Trevas","VTL 2.1"]},unlisted:!1,nextItem:{title:"Trevas - Provenance",permalink:"/Trevas/blog/trevas-provenance"}},d={authorsImageUrls:[void 0]},u=[];function v(e){const n={code:"code",li:"li",p:"p",ul:"ul",...(0,a.R)(),...e.components};return(0,t.jsxs)(t.Fragment,{children:[(0,t.jsx)(n.p,{children:"Trevas 1.7.0 upgrade to version 2.1 of VTL."}),"\n",(0,t.jsx)(n.p,{children:"This version introduces two new operators:"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:(0,t.jsx)(n.code,{children:"random"})}),"\n",(0,t.jsx)(n.li,{children:(0,t.jsx)(n.code,{children:"case"})}),"\n"]}),"\n",(0,t.jsxs)(n.p,{children:[(0,t.jsx)(n.code,{children:"random"})," produces a decimal number between 0 and 1."]}),"\n",(0,t.jsxs)(n.p,{children:[(0,t.jsx)(n.code,{children:"case"})," allows for clearer multi conditional branching, for example:"]}),"\n",(0,t.jsx)(n.p,{children:(0,t.jsx)(n.code,{children:'ds2 := ds1[ calc c := case when r < 0.2 then "Low" when r > 0.8 then "High" else "Medium" ]'})}),"\n",(0,t.jsx)(n.p,{children:"Both operators are already available in Trevas!"}),"\n",(0,t.jsx)(n.p,{children:"The new grammar also provides time operators and includes corrections, without any breaking changes compared to the 2.0 version."}),"\n",(0,t.jsxs)(n.p,{children:["See the ",(0,t.jsx)(o.A,{label:"coverage",href:(0,s.Ay)("/user-guide/coverage")})," section for more details."]})]})}function h(e={}){const{wrapper:n}={...(0,a.R)(),...e.components};return n?(0,t.jsx)(n,{...e,children:(0,t.jsx)(v,{...e})}):v(e)}},50116:(e,n,r)=>{r.d(n,{A:()=>a});r(96540);var t=r(74848);const a=e=>{let{label:n,href:r}=e;return(0,t.jsx)("a",{href:r,children:n})}},28453:(e,n,r)=>{r.d(n,{R:()=>o,x:()=>l});var t=r(96540);const a={},s=t.createContext(a);function o(e){const n=t.useContext(s);return t.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function l(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(a):e.components||a:o(e.components),t.createElement(s.Provider,{value:n},e.children)}}}]);