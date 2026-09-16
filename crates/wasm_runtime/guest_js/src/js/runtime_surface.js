(function installRuntimeSurface(global) {
  const UTF8_LABELS = new Set(["utf-8", "utf8", "unicode-1-1-utf-8"]);
  const BASE64_ALPHABET =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const REPLACEMENT_CHARACTER = "\uFFFD";

  const invalidCharacter = () => {
    const error = new Error("Invalid character");
    error.name = "InvalidCharacterError";
    throw error;
  };

  const encodeUtf8 = (source) => {
    const bytes = [];
    for (let index = 0; index < source.length; index++) {
      let codePoint = source.charCodeAt(index);
      if (codePoint >= 0xd800 && codePoint <= 0xdbff) {
        if (index + 1 < source.length) {
          const next = source.charCodeAt(index + 1);
          if (next >= 0xdc00 && next <= 0xdfff) {
            codePoint =
              0x10000 + ((codePoint - 0xd800) << 10) + (next - 0xdc00);
            index++;
          } else {
            codePoint = 0xfffd;
          }
        } else {
          codePoint = 0xfffd;
        }
      } else if (codePoint >= 0xdc00 && codePoint <= 0xdfff) {
        codePoint = 0xfffd;
      }

      if (codePoint <= 0x7f) {
        bytes.push(codePoint);
      } else if (codePoint <= 0x7ff) {
        bytes.push(0xc0 | (codePoint >> 6), 0x80 | (codePoint & 0x3f));
      } else if (codePoint <= 0xffff) {
        bytes.push(
          0xe0 | (codePoint >> 12),
          0x80 | ((codePoint >> 6) & 0x3f),
          0x80 | (codePoint & 0x3f),
        );
      } else {
        bytes.push(
          0xf0 | (codePoint >> 18),
          0x80 | ((codePoint >> 12) & 0x3f),
          0x80 | ((codePoint >> 6) & 0x3f),
          0x80 | (codePoint & 0x3f),
        );
      }
    }
    return new Uint8Array(bytes);
  };

  const codePointByteLength = (codePoint) => {
    if (codePoint <= 0x7f) {
      return 1;
    }
    if (codePoint <= 0x7ff) {
      return 2;
    }
    if (codePoint <= 0xffff) {
      return 3;
    }
    return 4;
  };

  const sourceCodePoint = (source, index) => {
    let codePoint = source.charCodeAt(index);
    let read = 1;
    if (codePoint >= 0xd800 && codePoint <= 0xdbff) {
      if (index + 1 < source.length) {
        const next = source.charCodeAt(index + 1);
        if (next >= 0xdc00 && next <= 0xdfff) {
          codePoint =
            0x10000 + ((codePoint - 0xd800) << 10) + (next - 0xdc00);
          read = 2;
        } else {
          codePoint = 0xfffd;
        }
      } else {
        codePoint = 0xfffd;
      }
    } else if (codePoint >= 0xdc00 && codePoint <= 0xdfff) {
      codePoint = 0xfffd;
    }
    return { codePoint, read };
  };

  class TextEncoder {
    get encoding() {
      return "utf-8";
    }

    encode(input = "") {
      return encodeUtf8(String(input));
    }

    encodeInto(source, destination) {
      if (!(destination instanceof Uint8Array)) {
        throw new TypeError("The destination must be a Uint8Array");
      }

      const value = String(source);
      let read = 0;
      let written = 0;
      while (read < value.length) {
        const { codePoint, read: codeUnits } = sourceCodePoint(value, read);
        const byteLength = codePointByteLength(codePoint);
        if (written + byteLength > destination.length) {
          break;
        }

        if (byteLength === 1) {
          destination[written] = codePoint;
        } else if (byteLength === 2) {
          destination[written] = 0xc0 | (codePoint >> 6);
          destination[written + 1] = 0x80 | (codePoint & 0x3f);
        } else if (byteLength === 3) {
          destination[written] = 0xe0 | (codePoint >> 12);
          destination[written + 1] = 0x80 | ((codePoint >> 6) & 0x3f);
          destination[written + 2] = 0x80 | (codePoint & 0x3f);
        } else {
          destination[written] = 0xf0 | (codePoint >> 18);
          destination[written + 1] = 0x80 | ((codePoint >> 12) & 0x3f);
          destination[written + 2] = 0x80 | ((codePoint >> 6) & 0x3f);
          destination[written + 3] = 0x80 | (codePoint & 0x3f);
        }
        read += codeUnits;
        written += byteLength;
      }
      return { read, written };
    }
  }

  const inputBytes = (input) => {
    if (input === undefined) {
      return new Uint8Array(0);
    }
    if (input instanceof ArrayBuffer) {
      return new Uint8Array(input);
    }
    if (ArrayBuffer.isView(input)) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    throw new TypeError("The input must be an ArrayBuffer or a view");
  };

  const appendBytes = (left, right) => {
    const bytes = new Uint8Array(left.length + right.length);
    bytes.set(left, 0);
    bytes.set(right, left.length);
    return bytes;
  };

  const decodeUtf8 = (bytes, fatal, stream) => {
    let output = "";
    let index = 0;
    let incompleteStart = -1;
    while (index < bytes.length) {
      const first = bytes[index];
      if (first <= 0x7f) {
        output += String.fromCharCode(first);
        index++;
        continue;
      }

      let needed;
      let codePoint;
      let lowerSecond = 0x80;
      let upperSecond = 0xbf;
      if (first >= 0xc2 && first <= 0xdf) {
        needed = 2;
        codePoint = first & 0x1f;
      } else if (first >= 0xe0 && first <= 0xef) {
        needed = 3;
        codePoint = first & 0x0f;
        if (first === 0xe0) {
          lowerSecond = 0xa0;
        } else if (first === 0xed) {
          upperSecond = 0x9f;
        }
      } else if (first >= 0xf0 && first <= 0xf4) {
        needed = 4;
        codePoint = first & 0x07;
        if (first === 0xf0) {
          lowerSecond = 0x90;
        } else if (first === 0xf4) {
          upperSecond = 0x8f;
        }
      } else {
        if (fatal) {
          throw new TypeError("The encoded data was not valid UTF-8");
        }
        output += REPLACEMENT_CHARACTER;
        index++;
        continue;
      }

      if (index + needed > bytes.length) {
        if (stream) {
          incompleteStart = index;
          break;
        }
        if (fatal) {
          throw new TypeError("The encoded data was not valid UTF-8");
        }
        output += REPLACEMENT_CHARACTER;
        index = bytes.length;
        continue;
      }

      const second = bytes[index + 1];
      if (second < lowerSecond || second > upperSecond) {
        if (fatal) {
          throw new TypeError("The encoded data was not valid UTF-8");
        }
        output += REPLACEMENT_CHARACTER;
        index++;
        continue;
      }
      codePoint = (codePoint << 6) | (second & 0x3f);

      let valid = true;
      for (let offset = 2; offset < needed; offset++) {
        const continuation = bytes[index + offset];
        if (continuation < 0x80 || continuation > 0xbf) {
          valid = false;
          if (fatal) {
            throw new TypeError("The encoded data was not valid UTF-8");
          }
          output += REPLACEMENT_CHARACTER;
          index += offset;
          break;
        }
        codePoint = (codePoint << 6) | (continuation & 0x3f);
      }
      if (!valid) {
        continue;
      }

      if (codePoint <= 0xffff) {
        output += String.fromCharCode(codePoint);
      } else {
        output += String.fromCodePoint(codePoint);
      }
      index += needed;
    }

    return {
      output,
      pending: incompleteStart === -1 ? new Uint8Array(0) : bytes.slice(incompleteStart),
    };
  };

  class TextDecoder {
    constructor(label = "utf-8", options = {}) {
      const normalized = String(label).trim().toLowerCase();
      if (!UTF8_LABELS.has(normalized)) {
        throw new RangeError(`The encoding label ${JSON.stringify(label)} is not supported`);
      }
      if (options === null || (typeof options !== "object" && typeof options !== "function")) {
        throw new TypeError("The options argument must be an object");
      }
      this._fatal = Boolean(options.fatal);
      this._ignoreBOM = Boolean(options.ignoreBOM);
      this._pending = new Uint8Array(0);
      this._bomSeen = false;
    }

    get encoding() {
      return "utf-8";
    }

    get fatal() {
      return this._fatal;
    }

    get ignoreBOM() {
      return this._ignoreBOM;
    }

    decode(input, options = {}) {
      if (options === null || (typeof options !== "object" && typeof options !== "function")) {
        throw new TypeError("The options argument must be an object");
      }
      const stream = Boolean(options.stream);
      const bytes = appendBytes(this._pending, inputBytes(input));
      const decoded = decodeUtf8(bytes, this._fatal, stream);
      this._pending = stream ? decoded.pending : new Uint8Array(0);

      let output = decoded.output;
      if (!this._bomSeen && (output.length > 0 || !stream)) {
        this._bomSeen = true;
        if (!this._ignoreBOM && output.charCodeAt(0) === 0xfeff) {
          output = output.slice(1);
        }
      }
      return output;
    }
  }

  const atob = (value) => {
    const input = String(value).replace(/[\t\n\f\r ]/g, "");
    if (input.length % 4 === 1 || /[^A-Za-z0-9+/=]/.test(input)) {
      return invalidCharacter();
    }

    const paddingIndex = input.indexOf("=");
    if (paddingIndex !== -1) {
      const padding = input.length - paddingIndex;
      if (
        padding > 2 ||
        input.length % 4 !== 0 ||
        !/^=+$/.test(input.slice(paddingIndex))
      ) {
        return invalidCharacter();
      }
    }

    let output = "";
    for (let index = 0; index < input.length; index += 4) {
      const first = BASE64_ALPHABET.indexOf(input[index]);
      const second = BASE64_ALPHABET.indexOf(input[index + 1]);
      if (first < 0 || second < 0) {
        return invalidCharacter();
      }
      const thirdChar = index + 2 < input.length ? input[index + 2] : "=";
      const fourthChar = index + 3 < input.length ? input[index + 3] : "=";
      const third = thirdChar === "=" ? 0 : BASE64_ALPHABET.indexOf(thirdChar);
      const fourth = fourthChar === "=" ? 0 : BASE64_ALPHABET.indexOf(fourthChar);
      if (third < 0 || fourth < 0) {
        return invalidCharacter();
      }
      output += String.fromCharCode((first << 2) | (second >> 4));
      if (thirdChar !== "=") {
        output += String.fromCharCode(((second & 0x0f) << 4) | (third >> 2));
      }
      if (fourthChar !== "=") {
        output += String.fromCharCode(((third & 0x03) << 6) | fourth);
      }
    }
    return output;
  };

  const btoa = (value) => {
    const input = String(value);
    let output = "";
    for (let index = 0; index < input.length; index += 3) {
      const first = input.charCodeAt(index);
      const hasSecond = index + 1 < input.length;
      const hasThird = index + 2 < input.length;
      const second = hasSecond ? input.charCodeAt(index + 1) : 0;
      const third = hasThird ? input.charCodeAt(index + 2) : 0;
      if (first > 0xff || second > 0xff || third > 0xff) {
        return invalidCharacter();
      }

      output += BASE64_ALPHABET[first >> 2];
      output += BASE64_ALPHABET[((first & 0x03) << 4) | (second >> 4)];
      output += hasSecond
        ? BASE64_ALPHABET[((second & 0x0f) << 2) | (third >> 6)]
        : "=";
      output += hasThird ? BASE64_ALPHABET[third & 0x3f] : "=";
    }
    return output;
  };

  Object.defineProperties(global, {
    TextEncoder: {
      configurable: true,
      enumerable: false,
      value: TextEncoder,
      writable: true,
    },
    TextDecoder: {
      configurable: true,
      enumerable: false,
      value: TextDecoder,
      writable: true,
    },
    atob: {
      configurable: true,
      enumerable: false,
      value: atob,
      writable: true,
    },
    btoa: {
      configurable: true,
      enumerable: false,
      value: btoa,
      writable: true,
    },
  });
})(globalThis);
