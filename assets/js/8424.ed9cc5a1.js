"use strict";
exports.id = 8424;
exports.ids = [8424];
exports.modules = {

/***/ 98424:
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "conf": () => (/* binding */ conf),
/* harmony export */   "language": () => (/* binding */ language)
/* harmony export */ });
/*!-----------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Version: 0.45.0(5e5af013f8d295555a7210df0d5f2cea0bf5dd56)
 * Released under the MIT license
 * https://github.com/microsoft/monaco-editor/blob/main/LICENSE.txt
 *-----------------------------------------------------------------------------*/

// src/basic-languages/twig/twig.ts
var conf = {
  wordPattern: /(-?\d*\.\d\w*)|([^\`\~\!\@\$\^\&\*\(\)\=\+\[\{\]\}\\\|\;\:\'\"\,\.\<\>\/\s]+)/g,
  comments: {
    blockComment: ["{#", "#}"]
  },
  brackets: [
    ["{#", "#}"],
    ["{%", "%}"],
    ["{{", "}}"],
    ["(", ")"],
    ["[", "]"],
    ["<!--", "-->"],
    ["<", ">"]
  ],
  autoClosingPairs: [
    { open: "{# ", close: " #}" },
    { open: "{% ", close: " %}" },
    { open: "{{ ", close: " }}" },
    { open: "[", close: "]" },
    { open: "(", close: ")" },
    { open: '"', close: '"' },
    { open: "'", close: "'" }
  ],
  surroundingPairs: [
    { open: '"', close: '"' },
    { open: "'", close: "'" },
    { open: "<", close: ">" }
  ]
};
var language = {
  defaultToken: "",
  tokenPostfix: "",
  ignoreCase: true,
  keywords: [
    "apply",
    "autoescape",
    "block",
    "deprecated",
    "do",
    "embed",
    "extends",
    "flush",
    "for",
    "from",
    "if",
    "import",
    "include",
    "macro",
    "sandbox",
    "set",
    "use",
    "verbatim",
    "with",
    "endapply",
    "endautoescape",
    "endblock",
    "endembed",
    "endfor",
    "endif",
    "endmacro",
    "endsandbox",
    "endset",
    "endwith",
    "true",
    "false"
  ],
  tokenizer: {
    root: [
      [/\s+/],
      [/{#/, "comment.twig", "@commentState"],
      [/{%[-~]?/, "delimiter.twig", "@blockState"],
      [/{{[-~]?/, "delimiter.twig", "@variableState"],
      [/<!DOCTYPE/, "metatag.html", "@doctype"],
      [/<!--/, "comment.html", "@comment"],
      [/(<)((?:[\w\-]+:)?[\w\-]+)(\s*)(\/>)/, ["delimiter.html", "tag.html", "", "delimiter.html"]],
      [/(<)(script)/, ["delimiter.html", { token: "tag.html", next: "@script" }]],
      [/(<)(style)/, ["delimiter.html", { token: "tag.html", next: "@style" }]],
      [/(<)((?:[\w\-]+:)?[\w\-]+)/, ["delimiter.html", { token: "tag.html", next: "@otherTag" }]],
      [/(<\/)((?:[\w\-]+:)?[\w\-]+)/, ["delimiter.html", { token: "tag.html", next: "@otherTag" }]],
      [/</, "delimiter.html"],
      [/[^<{]+/]
    ],
    commentState: [
      [/#}/, "comment.twig", "@pop"],
      [/./, "comment.twig"]
    ],
    blockState: [
      [/[-~]?%}/, "delimiter.twig", "@pop"],
      [/\s+/],
      [
        /(verbatim)(\s*)([-~]?%})/,
        ["keyword.twig", "", { token: "delimiter.twig", next: "@rawDataState" }]
      ],
      { include: "expression" }
    ],
    rawDataState: [
      [
        /({%[-~]?)(\s*)(endverbatim)(\s*)([-~]?%})/,
        ["delimiter.twig", "", "keyword.twig", "", { token: "delimiter.twig", next: "@popall" }]
      ],
      [/./, "string.twig"]
    ],
    variableState: [[/[-~]?}}/, "delimiter.twig", "@pop"], { include: "expression" }],
    stringState: [
      [/"/, "string.twig", "@pop"],
      [/#{\s*/, "string.twig", "@interpolationState"],
      [/[^#"\\]*(?:(?:\\.|#(?!\{))[^#"\\]*)*/, "string.twig"]
    ],
    interpolationState: [
      [/}/, "string.twig", "@pop"],
      { include: "expression" }
    ],
    expression: [
      [/\s+/],
      [/\+|-|\/{1,2}|%|\*{1,2}/, "operators.twig"],
      [/(and|or|not|b-and|b-xor|b-or)(\s+)/, ["operators.twig", ""]],
      [/==|!=|<|>|>=|<=/, "operators.twig"],
      [/(starts with|ends with|matches)(\s+)/, ["operators.twig", ""]],
      [/(in)(\s+)/, ["operators.twig", ""]],
      [/(is)(\s+)/, ["operators.twig", ""]],
      [/\||~|:|\.{1,2}|\?{1,2}/, "operators.twig"],
      [
        /[^\W\d][\w]*/,
        {
          cases: {
            "@keywords": "keyword.twig",
            "@default": "variable.twig"
          }
        }
      ],
      [/\d+(\.\d+)?/, "number.twig"],
      [/\(|\)|\[|\]|{|}|,/, "delimiter.twig"],
      [/"([^#"\\]*(?:\\.[^#"\\]*)*)"|\'([^\'\\]*(?:\\.[^\'\\]*)*)\'/, "string.twig"],
      [/"/, "string.twig", "@stringState"],
      [/=>/, "operators.twig"],
      [/=/, "operators.twig"]
    ],
    doctype: [
      [/[^>]+/, "metatag.content.html"],
      [/>/, "metatag.html", "@pop"]
    ],
    comment: [
      [/-->/, "comment.html", "@pop"],
      [/[^-]+/, "comment.content.html"],
      [/./, "comment.content.html"]
    ],
    otherTag: [
      [/\/?>/, "delimiter.html", "@pop"],
      [/"([^"]*)"/, "attribute.value.html"],
      [/'([^']*)'/, "attribute.value.html"],
      [/[\w\-]+/, "attribute.name.html"],
      [/=/, "delimiter.html"],
      [/[ \t\r\n]+/]
    ],
    script: [
      [/type/, "attribute.name.html", "@scriptAfterType"],
      [/"([^"]*)"/, "attribute.value.html"],
      [/'([^']*)'/, "attribute.value.html"],
      [/[\w\-]+/, "attribute.name.html"],
      [/=/, "delimiter.html"],
      [
        />/,
        {
          token: "delimiter.html",
          next: "@scriptEmbedded",
          nextEmbedded: "text/javascript"
        }
      ],
      [/[ \t\r\n]+/],
      [
        /(<\/)(script\s*)(>)/,
        ["delimiter.html", "tag.html", { token: "delimiter.html", next: "@pop" }]
      ]
    ],
    scriptAfterType: [
      [/=/, "delimiter.html", "@scriptAfterTypeEquals"],
      [
        />/,
        {
          token: "delimiter.html",
          next: "@scriptEmbedded",
          nextEmbedded: "text/javascript"
        }
      ],
      [/[ \t\r\n]+/],
      [/<\/script\s*>/, { token: "@rematch", next: "@pop" }]
    ],
    scriptAfterTypeEquals: [
      [
        /"([^"]*)"/,
        {
          token: "attribute.value.html",
          switchTo: "@scriptWithCustomType.$1"
        }
      ],
      [
        /'([^']*)'/,
        {
          token: "attribute.value.html",
          switchTo: "@scriptWithCustomType.$1"
        }
      ],
      [
        />/,
        {
          token: "delimiter.html",
          next: "@scriptEmbedded",
          nextEmbedded: "text/javascript"
        }
      ],
      [/[ \t\r\n]+/],
      [/<\/script\s*>/, { token: "@rematch", next: "@pop" }]
    ],
    scriptWithCustomType: [
      [
        />/,
        {
          token: "delimiter.html",
          next: "@scriptEmbedded.$S2",
          nextEmbedded: "$S2"
        }
      ],
      [/"([^"]*)"/, "attribute.value.html"],
      [/'([^']*)'/, "attribute.value.html"],
      [/[\w\-]+/, "attribute.name.html"],
      [/=/, "delimiter.html"],
      [/[ \t\r\n]+/],
      [/<\/script\s*>/, { token: "@rematch", next: "@pop" }]
    ],
    scriptEmbedded: [
      [/<\/script/, { token: "@rematch", next: "@pop", nextEmbedded: "@pop" }],
      [/[^<]+/, ""]
    ],
    style: [
      [/type/, "attribute.name.html", "@styleAfterType"],
      [/"([^"]*)"/, "attribute.value.html"],
      [/'([^']*)'/, "attribute.value.html"],
      [/[\w\-]+/, "attribute.name.html"],
      [/=/, "delimiter.html"],
      [
        />/,
        {
          token: "delimiter.html",
          next: "@styleEmbedded",
          nextEmbedded: "text/css"
        }
      ],
      [/[ \t\r\n]+/],
      [
        /(<\/)(style\s*)(>)/,
        ["delimiter.html", "tag.html", { token: "delimiter.html", next: "@pop" }]
      ]
    ],
    styleAfterType: [
      [/=/, "delimiter.html", "@styleAfterTypeEquals"],
      [
        />/,
        {
          token: "delimiter.html",
          next: "@styleEmbedded",
          nextEmbedded: "text/css"
        }
      ],
      [/[ \t\r\n]+/],
      [/<\/style\s*>/, { token: "@rematch", next: "@pop" }]
    ],
    styleAfterTypeEquals: [
      [
        /"([^"]*)"/,
        {
          token: "attribute.value.html",
          switchTo: "@styleWithCustomType.$1"
        }
      ],
      [
        /'([^']*)'/,
        {
          token: "attribute.value.html",
          switchTo: "@styleWithCustomType.$1"
        }
      ],
      [
        />/,
        {
          token: "delimiter.html",
          next: "@styleEmbedded",
          nextEmbedded: "text/css"
        }
      ],
      [/[ \t\r\n]+/],
      [/<\/style\s*>/, { token: "@rematch", next: "@pop" }]
    ],
    styleWithCustomType: [
      [
        />/,
        {
          token: "delimiter.html",
          next: "@styleEmbedded.$S2",
          nextEmbedded: "$S2"
        }
      ],
      [/"([^"]*)"/, "attribute.value.html"],
      [/'([^']*)'/, "attribute.value.html"],
      [/[\w\-]+/, "attribute.name.html"],
      [/=/, "delimiter.html"],
      [/[ \t\r\n]+/],
      [/<\/style\s*>/, { token: "@rematch", next: "@pop" }]
    ],
    styleEmbedded: [
      [/<\/style/, { token: "@rematch", next: "@pop", nextEmbedded: "@pop" }],
      [/[^<]+/, ""]
    ]
  }
};



/***/ })

};
;