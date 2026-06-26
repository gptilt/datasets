from dataclasses import replace
from dagster import ConfigurableResource
import gc
from pydantic import PrivateAttr
import torch
from typing import Literal
import whisperx
from whisperx.diarize import DiarizationPipeline


class Scribe(ConfigurableResource):
    """
    Reusable speech-to-text resource wrapping WhisperX + pyannote diarization.

    Steps performed per call:
    1. Lazy-load a Whisper ASR model (batched transcription) on first use.
    2. Transcribe the audio into segments, biased by an optional `initial_prompt`.
    3. Lazy-load and cache a WhisperX alignment model per language and refine
       word-level timestamps.
    4. Lazy-load a diarization model from Hugging Face and detect speaker segments.
    5. Assign each transcribed word to a speaker.

    Models stay resident across `transcribe()` calls within the same asset run, so a
    loop over many audio files reuses the VRAM allocation instead of reloading
    each time. VRAM is reclaimed when the Dagster run ends and the resource
    instance is discarded.

    Parameters
    ----------
    hugging_face_token : str
        Hugging Face API token used to access the pyannote.audio diarization model.
    device : str, default "cuda"
        Device to run models on ("cuda" for GPU, "cpu" for CPU).
    compute_type : Literal["int8", "float16"], default "float16"
        Precision type for model inference. Use "int8" for lower memory usage,
        "float16" for better accuracy.
    batch_size : int, default 4
        Batch size for Whisper transcription. Smaller values reduce GPU memory usage.
    models_directory : str, optional
        Directory to store or load Whisper model weights, to avoid repeated downloads.
        If None, defaults to WhisperX cache.
    """

    hugging_face_token: str
    device: str = "cuda"
    compute_type: Literal["int8", "float16"] = "float16"
    batch_size: int = 1
    models_directory: str | None = None

    _asr: object = PrivateAttr(default=None)
    _aligners: dict = PrivateAttr(default_factory=dict)
    _diarize: object = PrivateAttr(default=None)

    def transcribe(
        self,
        audio_file_path: str,
        initial_prompt: str | None = None,
    ) -> dict:
        """
        Transcribe an audio file, align word-level timestamps, and assign speaker labels.

        Parameters
        ----------
        audio_file_path : str
            Path to the input audio file to transcribe.
        initial_prompt : str, optional
            Context string fed to Whisper to bias ASR toward domain vocabulary.
            Swapped per call by mutating the cached ASR model's `options`.

        Returns
        -------
        dict
            Transcription with aligned word-level timestamps and speaker
            assignments. Each segment includes "start"/"end" timestamps, "text",
            and a speaker ID assigned to each word.
        """
        # Lazy-load ASR model on first call; reuse thereafter.
        if self._asr is None:
            self._asr = whisperx.load_model(
                "large-v2",
                self.device,
                compute_type=self.compute_type,
                download_root=self.models_directory,
            )

        # Swap initial_prompt on the cached pipeline's options dataclass.
        self._asr.options = replace(self._asr.options, initial_prompt=initial_prompt)

        audio = whisperx.load_audio(audio_file_path)
        transcription_result = self._asr.transcribe(audio, batch_size=self.batch_size)

        # Lazy-load and cache one alignment model per detected language.
        language = transcription_result["language"]
        if language not in self._aligners:
            self._aligners[language] = whisperx.load_align_model(
                language_code=language,
                device=self.device,
            )
        model_a, metadata = self._aligners[language]

        aligned = whisperx.align(
            transcription_result["segments"],
            model_a,
            metadata,
            audio,
            self.device,
            return_char_alignments=False,
        )

        # Lazy-load diarization model on first call; reuse thereafter.
        if self._diarize is None:
            self._diarize = DiarizationPipeline(
                token=self.hugging_face_token,
                device=self.device,
            )

        result = whisperx.assign_word_speakers(self._diarize(audio), aligned)

        # Cleanup
        del audio, transcription_result, aligned
        gc.collect()
        torch.cuda.empty_cache()

        # Return the final result
        return result
