import { defineConfig } from "@argo-video/cli";

export default defineConfig({
  baseURL: "http://localhost:5173",
  demosDir: "demos",
  outputDir: "videos",
  tts: {
    defaultVoice: "af_heart",
    defaultSpeed: 1.0,
  },
  video: {
    width: 1920,
    height: 1080,
    fps: 30,
    browser: "chromium",
  },
  export: {
    preset: "slow",
    crf: 18,
    // speedRamp: { gapSpeed: 3.0, minGapMs: 400 },  // disabled — filter graph issue with 10 scenes
    audio: { loudnorm: true },
    sharpen: true,
    // frame: {
    //   padding: 40,
    //   borderRadius: 14,
    //   shadow: 0.3,
    //   // background: { type: 'gradient', value: 'linear-gradient(135deg, #0f172a, #1e293b)' },
    // },
  },
  overlays: {
    autoBackground: false,
  },
});
